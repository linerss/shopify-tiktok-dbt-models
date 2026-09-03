/*
  Airbyte Shopify Order Line Items (store_a only for now)

  This model mirrors stg_shopify_order_line_items but reads from Airbyte sources.
  Key differences from Daton version:
  - LINE_ITEMS is native ARRAY (no try_parse_json needed)
  - Uses _airbyte_extracted_at instead of daton_batch_runtime
  - Materialized as table (Airbyte deduplicates, no append accumulation needed)
  - IDs cast to int::string to match Daton schema

  Output schema matches stg_shopify_order_line_items exactly for downstream compatibility.
*/

{{ config(
  materialized='table'
) }}

{% set brands = shopify_replacement_order_sources() %}

with
line_items_raw as (
    {% for b in brands %}
    select
        'Shopify' as source_platform,
        '{{ b.brand }}' as brand,

        -- Line item fields (from native ARRAY)
        o.id::int::string as order_id,
        parsed.value:"id"::int::string as line_item_id,
        parsed.value:"name"::string as item_name,
        parsed.value:"title"::string as product_title,
        parsed.value:"sku"::string as product_sku,
        parsed.value:"vendor"::string as vendor,
        parsed.value:"fulfillment_service"::string as fulfillment_service,
        parsed.value:"quantity"::float as quantity,
        parsed.value:"price"::float as product_price,
        parsed.value:"discount_allocations" as discount_allocations,

        -- Order timestamps
        o.processed_at,
        o.created_at,
        o.updated_at,
        o.cancelled_at,
        o.closed_at,

        -- Order details
        o.cancel_reason,
        o.confirmation_number,
        o.name,
        REPLACE(o.name, '#', '') as order_name_number,
        o.number,
        o.order_number,
        o.note,
        o.source_name,
        o.tags,

        -- Financial status
        o.financial_status,
        o.fulfillment_status,

        -- Pricing fields
        o.currency,
        o.presentment_currency,
        o.current_subtotal_price,
        o.current_total_discounts,
        o.current_total_price,
        o.current_total_tax,
        o.subtotal_price,
        o.total_discounts,
        o.total_line_items_price,
        o.total_price,
        o.total_price_usd,
        o.total_tax,
        o.total_outstanding,
        o.total_tip_received,
        o.total_weight,

        -- Pricing set fields (JSON objects with shop_money and presentment_money)
        o.current_subtotal_price_set,
        o.current_total_discounts_set,
        o.current_total_price_set,
        o.current_total_tax_set,
        o.subtotal_price_set,
        o.total_discounts_set,
        o.total_line_items_price_set,
        o.total_price_set,
        o.total_tax_set,
        o.total_shipping_price_set,

        -- Customer info
        -- Customer PII columns (email, name, phone, and street address) removed for open-source release.
        o.customer_locale,
        o.buyer_accepts_marketing,

        -- Address info (JSON objects)

        -- Payment info (Airbyte doesn't have gateway/processing_method/payment_details)
        null as gateway,
        o.payment_gateway_names,
        null as payment_details,
        null as processing_method,

        -- Discounts and codes
        o.discount_codes,
        o.discount_applications,

        -- Marketing attribution
        o.landing_site,
        o.landing_site_ref,
        o.referring_site,
        o.browser_ip,

        -- Tax info
        o.tax_exempt,
        o.taxes_included,
        o.tax_lines,
        null as estimated_taxes,  -- Airbyte may not have this field

        -- Fulfillment info

        -- Refunds
        o.refunds,

        -- IDs and references
        o.user_id,

        -- Flags
        o.test,

        -- Airbyte metadata (mapped to match Daton field names)
        null as daton_batch_id,
        extract(epoch from o._airbyte_extracted_at)::bigint * 1000 as daton_batch_runtime,
        null as daton_user_id,
        o._airbyte_extracted_at as daton_batch_date,
        date_trunc('day', o._airbyte_extracted_at) as ds

    from {{ source('airbyte_' ~ b.brand ~ '_shopify', b.source) }} o,
         lateral flatten(input => o.line_items) as parsed
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
),

discounts as (
    select
        'Shopify' as source_platform,
        order_id,
        line_item_id,
        sum(try_cast(da.value:"amount"::string as float)) as line_item_discount_total
    from (
        select distinct
            order_id,
            line_item_id,
            discount_allocations
        from line_items_raw
    ) li,
    lateral flatten(input => coalesce(li.discount_allocations, array_construct()), outer => true) as da
    where da.value is not null
    group by 1,2,3
),

base_data as (
    select
        li.*,
        coalesce(d.line_item_discount_total, 0) / nullif(li.quantity, 0) as line_item_discount_per_unit,
        li.processed_at as event_date,
        'sale' as row_type
    from line_items_raw li
    left join discounts d
        on li.order_id = d.order_id and li.line_item_id = d.line_item_id
),

unioned as (
    select * from base_data
),

order_notes as (
    {% for b in brands %}
    select
        '{{ b.brand }}' as brand,
        o.id::int::string as order_id,
        o.note_attributes,
        extract(epoch from o._airbyte_extracted_at)::bigint * 1000 as daton_batch_runtime
    from {{ source('airbyte_' ~ b.brand ~ '_shopify', b.source) }} o
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
),

order_keys as (
    select distinct
        brand,
        order_id,
        note_attributes
    from (
        select
            brand,
            order_id,
            note_attributes,
            row_number() over (
                partition by brand, order_id
                order by daton_batch_runtime desc
            ) as rn
        from order_notes
    )
    where rn = 1
),

exploded_attrs as (
    select
        k.brand,
        k.order_id,
        n.index as attr_index,
        n.value:"name"::string as attr_name,
        to_variant(n.value:"value") as attr_value
    from order_keys k,
         lateral flatten(input => coalesce(k.note_attributes, array_construct()), outer => true) n
    where n.value is not null
),

aggregated_attrs as (
    select
        brand,
        order_id,
        object_agg(attr_name, attr_value) as attrs_map
    from (
        select
            brand,
            order_id,
            attr_name,
            attr_value,
            row_number() over (
                partition by brand, order_id, attr_name
                order by attr_index desc
            ) as rn
        from exploded_attrs
    ) d
    where rn = 1
    group by brand, order_id
),

note_attributes_map as (
    select
        k.brand,
        k.order_id,
        coalesce(a.attrs_map, object_construct()) as note_attributes_map
    from order_keys k
    left join aggregated_attrs a
      on k.brand = a.brand and k.order_id = a.order_id
)

select
    u.*,

    --timestamps in PST (original value in TIMESTAMP_TZ)

    CONVERT_TIMEZONE('America/Los_Angeles', u.processed_at) AS processed_at_pst,
    CONVERT_TIMEZONE('America/Los_Angeles', u.event_date) AS event_date_pst,
    CONVERT_TIMEZONE('America/Los_Angeles', u.updated_at) AS updated_at_pst,
    CONVERT_TIMEZONE('America/Los_Angeles', u.cancelled_at) AS cancelled_pst,
    CONVERT_TIMEZONE('America/Los_Angeles', u.closed_at) AS closed_at_pst,
    -- note_attributes_map

    nam.note_attributes_map:clientOrderId::string as client_order_id,
    nam.note_attributes_map:orderType::string as order_type,
    nam.note_attributes_map:cardLast4::string as card_last4,
    nam.note_attributes_map:utm_source::string as utm_source,
    nam.note_attributes_map:utm_medium::string as utm_medium,
    nam.note_attributes_map:utm_campaign::string as utm_campaign,
    nam.note_attributes_map:utm_term::string as utm_term,
    nam.note_attributes_map:utm_content::string as utm_content,
    nam.note_attributes_map:custom1::string as custom1,
    nam.note_attributes_map:custom2::string as custom2,
    nam.note_attributes_map as note_attributes_map
from unioned u
left join note_attributes_map nam
  on u.brand = nam.brand
 and u.order_id = nam.order_id
