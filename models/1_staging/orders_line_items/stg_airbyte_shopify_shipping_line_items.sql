/*
  Airbyte Shopify Shipping Line Items (store_a only for now)

  This model mirrors stg_shopify_shipping_line_items but reads from Airbyte sources.
  Key differences from Daton version:
  - SHIPPING_LINES is native ARRAY (no parse_json needed)
  - Uses _airbyte_extracted_at instead of daton_batch_runtime
  - Materialized as table (Airbyte deduplicates, no append accumulation needed)

  Filters out TikTok orders (email domain contains 'tiktok').
  Output schema matches stg_shopify_shipping_line_items exactly for downstream compatibility.
*/

{{ config(
  materialized='table'
) }}

{% set brands = shopify_replacement_order_sources() %}

with
source as (
    {% for b in brands %}
    select
        'Shopify' as source_platform,
        upper('{{ b.brand }}') as brand,

        -- Shipping line fields (from native ARRAY)
        o.id::int::string as order_id,
        parsed.value:"id"::int::string as shipping_line_id,
        parsed.value:"title"::string as shipping_title,
        parsed.value:"price"::float as shipping_price,
        parsed.value:"code"::string as shipping_code,
        parsed.value:"carrier_identifier"::string as carrier_identifier,
        parsed.value:"source"::string as shipping_source,
        parsed.value:"is_removed"::boolean as is_removed,
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
        o.name as order_name,
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

        -- Pricing set fields (JSON objects)
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

    from {{ source('airbyte_' ~ b.brand ~ '_shopify', b.source) }} o
    cross join lateral flatten(input => o.shipping_lines) as parsed
    where not ilike(split_part(o.email, '@', 2), '%tiktok%')
      and o.shipping_lines is not null
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
),

shipping_discounts as (
    select
        order_id,
        shipping_line_id,
        sum(da.value:"amount"::float) as shipping_discount
    from (
        select distinct
            order_id,
            shipping_line_id,
            discount_allocations
        from source
    ) s,
    lateral flatten(input => coalesce(s.discount_allocations, array_construct())) as da
    group by order_id, shipping_line_id
),

combined as (
    select
        s.*,
        sd.shipping_discount,
        s.processed_at as event_date,
        'sale' as row_type
    from source s
    left join shipping_discounts sd
        on s.order_id = sd.order_id and s.shipping_line_id = sd.shipping_line_id
),

order_notes as (
    {% for b in brands %}
    select
        upper('{{ b.brand }}') as brand,
        o.id::int::string as order_id,
        o.note_attributes
    from {{ source('airbyte_' ~ b.brand ~ '_shopify', b.source) }} o
    where not ilike(split_part(o.email, '@', 2), '%tiktok%')
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
),

order_keys as (
    select distinct brand, order_id, note_attributes from order_notes
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
from combined u
left join note_attributes_map nam
  on u.brand = nam.brand
 and u.order_id = nam.order_id
