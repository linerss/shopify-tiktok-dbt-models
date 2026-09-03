{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns'
) }}

{# Customer contact and street-address fields are intentionally not selected. #}
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
    {{ normalize_numeric_id('shipping.value:"id"') }} as shipping_line_id,
    shipping.value:"title"::string as shipping_title,
    shipping.value:"code"::string as shipping_code,
    shipping.value:"source"::string as shipping_source,
    shipping.value:"price"::number(18, 4) as shipping_price,
    shipping.value:"discounted_price"::number(18, 4) as discounted_shipping_price,
    src.value:"currency"::string as currency,
    src.value:"shipping_address":"country_code"::string as shipping_country_code,
    src.value:"shipping_address":"province_code"::string as shipping_province_code,
    src.value:"created_at"::timestamp_tz as created_at,
    src.value:"updated_at"::timestamp_tz as updated_at,
    to_timestamp_tz(src.source_batch_runtime_ms / 1000) as source_loaded_at
from {{ source('shopify_legacy', store.table) }} as src,
lateral flatten(input => try_parse_json(src.value:"shipping_lines"::string)) as shipping
{% if is_incremental() %}
where to_timestamp_tz(src.source_batch_runtime_ms / 1000) > (
    select coalesce(max(source_loaded_at), '1900-01-01'::timestamp_tz)
    from {{ this }}
    where store_id = '{{ store.store_id }}'
)
{% endif %}

{% if not loop.last %}union all{% endif %}
{% endfor %}
