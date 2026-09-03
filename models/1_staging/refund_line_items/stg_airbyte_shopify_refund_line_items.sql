/*
  Airbyte Shopify Refund Line Items (store_a only for now)

  This model mirrors stg_shopify_refund_line_items but reads from Airbyte sources.
  Key differences from Daton version:
  - REFUND_LINE_ITEMS is native ARRAY (no parse_json needed)
  - line_item is OBJECT not ARRAY: use parsed.value:line_item:title instead of parsed.value:line_item[0]:title
  - ORDER_ADJUSTMENTS is native ARRAY
  - Uses _airbyte_extracted_at instead of daton_batch_runtime
  - Materialized as table (Airbyte deduplicates)

  NOTE: refund_shipping_lines may not be available in Airbyte ORDER_REFUNDS table.
  This needs verification - the CTE is included but may return zero rows.

  Output schema matches stg_shopify_refund_line_items exactly for downstream compatibility.
*/

{{
  config(
    materialized = "table"
  )
}}

{% set brands = shopify_replacement_refund_sources() %}

with
{% for b in brands %}
base_{{ loop.index }} as (
    select
        id,
        order_id,
        created_at,
        note,
        refund_line_items,
        order_adjustments,
        -- refund_shipping_lines may not exist in Airbyte - handle gracefully
        try_cast(null as array) as refund_shipping_lines,  -- Placeholder until we verify the schema
        _airbyte_extracted_at,
        '{{ b.brand }}' as brand
    from {{ source('airbyte_' ~ b.brand ~ '_shopify', b.source) }}
    where (refund_line_items is not null
       or order_adjustments is not null)
),

order_adjustments_{{ loop.index }} as (
    select
        -- Core identifiers
        'Shopify' as source_platform,
        order_id::int::string as order_id,
        id::int::string as refund_record_id,
        'adjustment' as record_type,
        parsed.value:id::int::string as refund_line_item_id,
        created_at,
        note,

        -- Adjustment details
        parsed.value:amount::float as a_amount,
        parsed.value:kind::string as a_kind,
        parsed.value:reason::string as a_reason,
        parsed.value:refund_id::int::string as a_linked_refund_id,
        parsed.value:tax_amount::float as a_tax_amount,
        -- Airbyte may have different nested structure for amount_set
        coalesce(
            parsed.value:amount_set:presentment_money:currency_code::string,
            parsed.value:amount_set[0]:presentment_money[0]:currency_code::string
        ) as a_currency_code,

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

        -- Timestamps (mapped to match Daton field names)
        _airbyte_extracted_at as daton_batch_date,
        date_trunc('day', _airbyte_extracted_at) as ds,

        -- Brand identifier
        brand
    from base_{{ loop.index }}
    cross join lateral flatten(input => order_adjustments) as parsed
    where order_adjustments is not null
),

refund_line_items_{{ loop.index }} as (
    select
        -- Core identifiers
        'Shopify' as source_platform,
        order_id::int::string as order_id,
        id::int::string as refund_record_id,
        'refund_line_item' as record_type,
        parsed.value:id::int::string as refund_line_item_id,
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
        -- In Airbyte, line_item is OBJECT not ARRAY (unlike Daton's line_item[0])
        coalesce(
            parsed.value:line_item:title::string,
            parsed.value:line_item[0]:title::string
        ) as rli_product_title,
        coalesce(
            parsed.value:line_item:sku::string,
            parsed.value:line_item[0]:sku::string
        ) as rli_product_sku,
        coalesce(
            parsed.value:line_item:price::float,
            parsed.value:line_item[0]:price::float
        ) as rli_product_price,
        parsed.value:quantity::integer as rli_product_quantity,
        parsed.value:restock_type::string as rli_restock_type,
        parsed.value:subtotal::float as rli_refund_subtotal,
        parsed.value:total_tax::float as rli_refund_total_tax,
        coalesce(
            parsed.value:subtotal_set:presentment_money:currency_code::string,
            parsed.value:subtotal_set[0]:presentment_money[0]:currency_code::string
        ) as rli_currency_code,
        parsed.value:subtotal::float as rli_refund_subtotal_total_line,
        case when nullif(parsed.value:quantity::integer, 0) is not null
             then parsed.value:subtotal::float / nullif(parsed.value:quantity::integer, 0)
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
        _airbyte_extracted_at as daton_batch_date,
        date_trunc('day', _airbyte_extracted_at) as ds,

        -- Brand identifier
        brand
    from base_{{ loop.index }}
    cross join lateral flatten(input => refund_line_items) as parsed
    where refund_line_items is not null
)

-- NOTE: refund_shipping_lines CTE omitted for now as it may not exist in Airbyte ORDER_REFUNDS.
-- If shipping refunds exist, they may be embedded differently. Add back after schema verification.

{% if not loop.last %},{% endif %}
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
    -- refund_shipping_lines would go here if available
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
)
qualify row_number() over (partition by brand, refund_record_id, refund_line_item_id, record_type order by daton_batch_date desc) = 1
