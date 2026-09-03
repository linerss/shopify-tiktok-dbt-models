{{ config(
  materialized='incremental',
  incremental_strategy='append',
  on_schema_change='sync_all_columns'
) }}

-- Daton switched shipping line IDs to decimal strings on <CUTOVER_DATE>; cast to int to normalize.

{% set brands = shopify_legacy_order_sources() %}

with
{% if is_incremental() %}
max_timestamps as (
    select brand, max(updated_at) as max_date
    from {{ this }}
    group by brand
),
{% endif %}

source as (
    {% for b in brands %}
    select
        'Shopify' as source_platform,
        upper('{{ b.brand }}') as brand,

        -- Shipping line fields (from parsed JSON)
        o.id::int::string as order_id,
        parsed.value:"id"::int::string as shipping_line_id,  -- Cast to int first to normalize decimal formats (e.g., 123.000000000 -> 123)
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

        -- Payment info
        o.gateway,
        o.payment_gateway_names,
        o.payment_details,
        o.processing_method,

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
        o.estimated_taxes,

        -- Fulfillment info

        -- Refunds
        o.refunds,

        -- IDs and references
        o.user_id,

        -- Flags
        o.test,

        -- Daton metadata
        o.daton_batch_id,
        o.daton_batch_runtime,
        o.daton_user_id,
        to_timestamp(o.daton_batch_runtime/1000) as daton_batch_date,
        date_trunc('day', to_timestamp(o.daton_batch_runtime/1000)) as ds

    from {{ source('shopify', b.source) }} o
    cross join lateral flatten(input => parse_json(o.shipping_lines)) as parsed
    where not ilike(split_part(o.email, '@', 2), '%tiktok%')
      and o.shipping_lines is not null
    {% if is_incremental() %}
      and o.updated_at >= (
          select max_date
          from max_timestamps
          where brand = upper('{{ b.brand }}')
      )
    {% endif %}
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
    lateral flatten(input => coalesce(try_parse_json(s.discount_allocations), array_construct())) as da
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
    from {{ source('shopify', b.source) }} o
    where not ilike(split_part(o.email, '@', 2), '%tiktok%')
    {% if is_incremental() %}
      and o.updated_at >= (
          select max_date
          from max_timestamps
          where brand = upper('{{ b.brand }}')
      )
    {% endif %}
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
         lateral flatten(input => try_parse_json(k.note_attributes), outer => true) n
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