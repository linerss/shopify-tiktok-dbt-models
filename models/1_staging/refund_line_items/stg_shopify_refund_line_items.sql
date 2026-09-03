/*
  Shopify Refunds (Shopify orders only, excludes TikTok)

  This model extracts refund data from Shopify for direct Shopify customer refunds.

  NOTE: TikTok refunds are NOT included here - they are handled as cancellations
  in the TikTok order line items models (stg/int_tiktok_order_line_items).
*/

{{
  config(
    materialized = "incremental",
    incremental_strategy = "merge",
    unique_key = ['brand', 'refund_record_id', 'refund_line_item_id', 'record_type'],
    on_schema_change = 'sync_all_columns'
  )
}}

-- Daton switched refund-related IDs to decimal strings on <CUTOVER_DATE>; cast to int to normalize.

{% set brands = shopify_legacy_refund_sources() %}

with
{% if is_incremental() %}
max_timestamps as (
    select
        brand,
        max(created_at) as max_date
    from {{ this }}
    group by brand
),
{% endif %}

{% for b in brands %}
base_{{ loop.index }} as (
    select
        id,
        order_id,
        created_at,
        note,
        refund_line_items,
        order_adjustments,
        refund_shipping_lines,
        daton_batch_runtime,
        '{{ b.brand }}' as brand
    from {{ source('shopify', b.source) }}
    where (refund_line_items is not null
       or order_adjustments is not null
       or refund_shipping_lines is not null)
    {% if is_incremental() %}
    and created_at >= (
        select max_date
        from max_timestamps
        where brand = '{{ b.brand }}'
    )
    {% endif %}
),

order_adjustments_{{ loop.index }} as (
    select
        -- Core identifiers
        'Shopify' as source_platform,
        order_id,
        id as refund_record_id,
        'adjustment' as record_type,
        parsed.value:id::int::string as refund_line_item_id,  -- Cast to int first to normalize decimal formats
        created_at,
        note,

        -- Adjustment details
        parsed.value:amount::float as a_amount,
        parsed.value:kind::string as a_kind,
        parsed.value:reason::string as a_reason,
        parsed.value:refund_id::int::string as a_linked_refund_id,  -- Cast to int first to normalize decimal formats
        parsed.value:tax_amount::float as a_tax_amount,
        parsed.value:amount_set[0]:presentment_money[0]:currency_code::string as a_currency_code,

        -- Product fields (null for adjustments)
        null as rli_product_title,
        null as rli_product_sku,
        null as rli_product_price,
        null as rli_product_quantity,
        null as rli_restock_type,
        null as rli_refund_subtotal,
        null as rli_refund_total_tax,
        null as rli_currency_code,
        null as rli_refund_subtotal_total_line,
        null as rli_refund_subtotal_per_unit,

        -- Shipping fields (null for adjustments)
        null as rsl_shipping_title,
        null as rsl_shipping_price,
        null as rsl_shipping_discounted_price,
        null as rsl_shipping_source,
        null as rsl_currency_code,
        null as rsl_shipping_discounted_price_total,
        null as rsl_shipping_discounted_price_per_unit,

        -- Timestamps
        to_timestamp(daton_batch_runtime/1000) as daton_batch_date,
        date_trunc('day', to_timestamp(daton_batch_runtime/1000)) as ds,

        -- Brand identifier
        brand
    from base_{{ loop.index }}
    cross join lateral flatten(input => parse_json(order_adjustments)) as parsed
),

refund_line_items_{{ loop.index }} as (
    select
        -- Core identifiers
        'Shopify' as source_platform,
        order_id,
        id as refund_record_id,
        'refund_line_item' as record_type,
        parsed.value:id::int::string as refund_line_item_id,  -- Cast to int first to normalize decimal formats
        created_at,
        note,

        -- Adjustment fields (null for line items)
        null as a_amount,
        null as a_kind,
        null as a_reason,
        null as a_linked_refund_id,
        null as a_tax_amount,
        null as a_currency_code,

        -- Product details
        parsed.value:line_item[0]:title::string as rli_product_title,
        parsed.value:line_item[0]:sku::string as rli_product_sku,
        parsed.value:line_item[0]:price::float as rli_product_price,
        parsed.value:quantity::integer as rli_product_quantity,
        parsed.value:restock_type::string as rli_restock_type,
        parsed.value:subtotal::float as rli_refund_subtotal,
        parsed.value:total_tax::float as rli_refund_total_tax,
        parsed.value:subtotal_set[0]:presentment_money[0]:currency_code::string as rli_currency_code,
        parsed.value:subtotal::float as rli_refund_subtotal_total_line,
        case when nullif(parsed.value:quantity::integer,0) is not null
             then parsed.value:subtotal::float / nullif(parsed.value:quantity::integer,0)
        end as rli_refund_subtotal_per_unit,

        -- Shipping fields (null for line items)
        null as rsl_shipping_title,
        null as rsl_shipping_price,
        null as rsl_shipping_discounted_price,
        null as rsl_shipping_source,
        null as rsl_currency_code,
        null as rsl_shipping_discounted_price_total,
        null as rsl_shipping_discounted_price_per_unit,

        -- Timestamps
        to_timestamp(daton_batch_runtime/1000) as daton_batch_date,
        date_trunc('day', to_timestamp(daton_batch_runtime/1000)) as ds,

        -- Brand identifier
        brand
    from base_{{ loop.index }}
    cross join lateral flatten(input => parse_json(refund_line_items)) as parsed
),

refund_shipping_lines_{{ loop.index }} as (
    select
        -- Core identifiers
        'Shopify' as source_platform,
        order_id,
        id as refund_record_id,
        'refund_shipping_line' as record_type,
        parsed.value:id::int::string as refund_line_item_id,  -- Cast to int first to normalize decimal formats
        created_at,
        note,

        -- Adjustment fields (null for shipping)
        null as a_amount,
        null as a_kind,
        null as a_reason,
        null as a_linked_refund_id,
        null as a_tax_amount,
        null as a_currency_code,

        -- Product fields (null for shipping)
        null as rli_product_title,
        null as rli_product_sku,
        null as rli_product_price,
        null as rli_product_quantity,
        null as rli_restock_type,
        null as rli_refund_subtotal,
        null as rli_refund_total_tax,
        null as rli_currency_code,
        null as rli_refund_subtotal_total_line,
        null as rli_refund_subtotal_per_unit,

        -- Shipping details
        parsed.value:shipping_line[0]:title::string as rsl_shipping_title,
        parsed.value:shipping_line[0]:price::float as rsl_shipping_price,
        parsed.value:shipping_line[0]:discounted_price::float as rsl_shipping_discounted_price,
        parsed.value:shipping_line[0]:source::string as rsl_shipping_source,
        parsed.value:price_set[0]:presentment_money[0]:currency_code::string as rsl_currency_code,
        parsed.value:shipping_line[0]:discounted_price::float as rsl_shipping_discounted_price_total,
        parsed.value:shipping_line[0]:discounted_price::float as rsl_shipping_discounted_price_per_unit,

        -- Timestamps
        to_timestamp(daton_batch_runtime/1000) as daton_batch_date,
        date_trunc('day', to_timestamp(daton_batch_runtime/1000)) as ds,

        -- Brand identifier
        brand
    from base_{{ loop.index }}
    cross join lateral flatten(input => parse_json(refund_shipping_lines)) as parsed
){% if not loop.last %},{% endif %}
{% endfor %}

select
    source_platform,
    order_id,
    refund_record_id,
    record_type,
    refund_line_item_id,
    created_at,
    note,
    a_amount, a_kind, a_reason, a_linked_refund_id, a_tax_amount, a_currency_code,
    rli_product_title, rli_product_sku, rli_product_price, rli_product_quantity, rli_restock_type,
    rli_refund_subtotal, rli_refund_total_tax, rli_currency_code,
    rli_refund_subtotal_total_line, rli_refund_subtotal_per_unit,
    rsl_shipping_title, rsl_shipping_price, rsl_shipping_discounted_price, rsl_shipping_source, rsl_currency_code,
    rsl_shipping_discounted_price_total, rsl_shipping_discounted_price_per_unit,
    daton_batch_date, ds, brand
from (
    {% for b in brands %}
    select * from order_adjustments_{{ loop.index }}
    union all
    select * from refund_line_items_{{ loop.index }}
    union all
    select * from refund_shipping_lines_{{ loop.index }}
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
)
qualify row_number() over (partition by brand, refund_record_id, refund_line_item_id, record_type order by daton_batch_date desc) = 1
