{{ config(
    materialized='incremental',
    incremental_strategy='append',
    on_schema_change='append_new_columns'
) }}

{#
  TikTok exposes shipping at order grain. This adapter treats the first payment
  object as authoritative; validate that assumption against connector docs.
  Customer contact details, tracking identifiers, and full addresses are omitted.
#}
{% set stores = var('tiktok_shop_stores', [
    {'store_id': 'store_a', 'orders_table': 'store_a_orders'},
    {'store_id': 'store_b', 'orders_table': 'store_b_orders'}
]) %}

{% for store in stores %}
select
    'tiktok_shop' as source_platform,
    '{{ store.store_id }}' as store_id,
    {{ normalize_numeric_id('src.value:"order_id"') }} as order_id,
    concat({{ normalize_numeric_id('src.value:"order_id"') }}, '-shipping') as shipping_line_id,
    payment.value:"shipping_fee"::number(18, 4) as shipping_fee,
    payment.value:"shipping_fee_platform_discount"::number(18, 4) as platform_shipping_discount,
    payment.value:"shipping_fee_seller_discount"::number(18, 4) as seller_shipping_discount,
    src.value:"currency"::string as currency,
    src.value:"recipient_address":"region_code"::string as shipping_country_code,
    src.value:"recipient_address":"state_code"::string as shipping_province_code,
    src.value:"order_status"::string as order_status,
    src.value:"create_time"::timestamp_tz as created_at,
    src.value:"update_time"::timestamp_tz as updated_at,
    to_timestamp_tz(src.source_batch_runtime_ms / 1000) as source_loaded_at
from {{ source('tiktok_shop', store.orders_table) }} as src,
lateral flatten(
    input => try_parse_json(src.value:"payment"::string),
    outer => true
) as payment
where payment.index = 0 or payment.index is null
{% if is_incremental() %}
and to_timestamp_tz(src.source_batch_runtime_ms / 1000) > (
    select coalesce(max(source_loaded_at), '1900-01-01'::timestamp_tz)
    from {{ this }}
    where store_id = '{{ store.store_id }}'
)
{% endif %}

{% if not loop.last %}union all{% endif %}
{% endfor %}
