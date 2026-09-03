{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns'
) }}

{# Free-text return reasons are intentionally excluded. #}
{% set stores = var('tiktok_shop_stores', [
    {'store_id': 'store_a', 'returns_table': 'store_a_returns'},
    {'store_id': 'store_b', 'returns_table': 'store_b_returns'}
]) %}

{% for store in stores %}
select
    'tiktok_shop' as source_platform,
    '{{ store.store_id }}' as store_id,
    {{ normalize_numeric_id('src.value:"return_id"') }} as return_id,
    {{ normalize_numeric_id('src.value:"order_id"') }} as order_id,
    {{ normalize_numeric_id('item.value:"sku_id"') }} as line_item_id,
    src.value:"return_type"::string as return_type,
    src.value:"return_status"::string as return_status,
    case
        when item.index is null then src.value:"refund_amount"::number(18, 4)
        else item.value:"refund_amount"::number(18, 4)
    end as refund_amount,
    src.value:"currency"::string as currency,
    src.value:"create_time"::timestamp_tz as created_at,
    src.value:"update_time"::timestamp_tz as updated_at,
    to_timestamp_tz(src.source_batch_runtime_ms / 1000) as source_loaded_at
from {{ source('tiktok_shop', store.returns_table) }} as src,
lateral flatten(input => try_parse_json(src.value:"return_line_items"::string), outer => true) as item
{% if is_incremental() %}
where to_timestamp_tz(src.source_batch_runtime_ms / 1000) > (
    select coalesce(max(source_loaded_at), '1900-01-01'::timestamp_tz)
    from {{ this }}
    where store_id = '{{ store.store_id }}'
)
{% endif %}

{% if not loop.last %}union all{% endif %}
{% endfor %}
