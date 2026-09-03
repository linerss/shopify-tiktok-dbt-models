{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns'
) }}

{# Append every connector snapshot. Current-state deduplication belongs downstream. #}
{% set stores = var('shopify_legacy_stores', [
    {'store_id': 'store_a', 'table': 'store_a_orders'},
    {'store_id': 'store_b', 'table': 'store_b_orders'}
]) %}

{% for store in stores %}
select
    'shopify' as source_platform,
    'legacy' as connector,
    '{{ store.store_id }}' as store_id,
    {{ normalize_numeric_id('src.value:"id"') }} as order_id,
    {{ normalize_numeric_id('line.value:"id"') }} as line_item_id,
    {{ normalize_numeric_id('line.value:"product_id"') }} as product_id,
    {{ normalize_numeric_id('line.value:"variant_id"') }} as variant_id,
    line.value:"sku"::string as sku,
    line.value:"title"::string as product_title,
    line.value:"quantity"::number as quantity,
    line.value:"price"::number(18, 4) as unit_price,
    line.value:"total_discount"::number(18, 4) as line_discount,
    src.value:"currency"::string as currency,
    src.value:"financial_status"::string as financial_status,
    line.value:"fulfillment_status"::string as fulfillment_status,
    src.value:"cancelled_at"::timestamp_tz as cancelled_at,
    src.value:"created_at"::timestamp_tz as created_at,
    src.value:"updated_at"::timestamp_tz as updated_at,
    to_timestamp_tz(src.source_batch_runtime_ms / 1000) as source_loaded_at
from {{ source('shopify_legacy', store.table) }} as src,
lateral flatten(input => try_parse_json(src.value:"line_items"::string)) as line
{% if is_incremental() %}
where to_timestamp_tz(src.source_batch_runtime_ms / 1000) > (
    select coalesce(max(source_loaded_at), '1900-01-01'::timestamp_tz)
    from {{ this }}
    where store_id = '{{ store.store_id }}'
)
{% endif %}

{% if not loop.last %}union all{% endif %}
{% endfor %}
