{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns'
) }}

{# Append snapshots so financial and operational state can be modeled separately. #}
{% set stores = var('tiktok_shop_stores', [
    {'store_id': 'store_a', 'orders_table': 'store_a_orders'},
    {'store_id': 'store_b', 'orders_table': 'store_b_orders'}
]) %}

{% for store in stores %}
select
    'tiktok_shop' as source_platform,
    '{{ store.store_id }}' as store_id,
    {{ normalize_numeric_id('src.value:"order_id"') }} as order_id,
    {{ normalize_numeric_id('line.value:"sku_id"') }} as line_item_id,
    {{ normalize_numeric_id('line.value:"product_id"') }} as product_id,
    line.value:"seller_sku"::string as sku,
    line.value:"product_name"::string as product_title,
    coalesce(line.value:"quantity"::number, 1) as quantity,
    line.value:"original_price"::number(18, 4) as original_unit_price,
    line.value:"sale_price"::number(18, 4) as sale_unit_price,
    line.value:"platform_discount"::number(18, 4) as platform_discount,
    line.value:"seller_discount"::number(18, 4) as seller_discount,
    src.value:"currency"::string as currency,
    src.value:"order_status"::string as order_status,
    src.value:"create_time"::timestamp_tz as created_at,
    src.value:"paid_time"::timestamp_tz as paid_at,
    src.value:"update_time"::timestamp_tz as updated_at,
    to_timestamp_tz(src.source_batch_runtime_ms / 1000) as source_loaded_at
from {{ source('tiktok_shop', store.orders_table) }} as src,
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
