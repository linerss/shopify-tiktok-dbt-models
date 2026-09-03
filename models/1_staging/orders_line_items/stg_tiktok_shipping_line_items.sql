{{ config(
  materialized='incremental',
  incremental_strategy='append',
  on_schema_change='sync_all_columns'
) }}

{% set brands = tiktok_order_sources() %}

with
{% if is_incremental() %}
max_timestamps as (
    select brand, max(update_time) as max_date
    from {{ this }}
    group by brand
),
{% endif %}

source_raw as (
    {% for b in brands %}
    select
        'TikTok' as source_platform,
        upper('{{ b.brand }}') as brand,
        o.id::string as order_id,

        -- Shipping-level fields
        parse_json(o.payment)[0]:shipping_fee::float as shipping_fee,
        parse_json(o.payment)[0]:shipping_fee_cofunded_discount::float as shipping_fee_cofunded_discount,
        parse_json(o.payment)[0]:shipping_fee_platform_discount::float as shipping_fee_platform_discount,
        parse_json(o.payment)[0]:shipping_fee_seller_discount::float as shipping_fee_seller_discount,
        parse_json(o.payment)[0]:original_shipping_fee::float as original_shipping_fee,
        parse_json(o.payment)[0]:currency::string as currency,

        -- Order metadata
        o.tracking_number as order_tracking_number,
        o.delivery_option_name,
        o.fulfillment_type,
        -- Customer PII columns (email, name, phone, and street address) removed for open-source release.
        o.status as order_status,
        o.cancel_reason,

        -- Timestamps (UTC)
        to_timestamp(o.cancel_time) as cancel_time,
        to_timestamp(o.delivery_time/1000) as order_delivery_time,
        to_timestamp(o.paid_time) as paid_time,
        to_timestamp(o.create_time) as create_time,
        to_timestamp(o.update_time) as update_time,
        to_timestamp(o.daton_batch_runtime/1000) as daton_batch_date,

        -- Metadata
        date_trunc('day', to_timestamp(o.daton_batch_runtime/1000)) as ds

    from {{ source('tiktok', b.source) }} o

    {% if is_incremental() %}
    where to_timestamp(o.update_time) >= (
        select max_date
        from max_timestamps
        where brand = upper('{{ b.brand }}')
    )
    {% endif %}
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
)

select
    source_platform,
    brand,
    order_id,

    -- Shipping fields
    shipping_fee,
    shipping_fee_cofunded_discount,
    shipping_fee_platform_discount,
    shipping_fee_seller_discount,
    original_shipping_fee,
    currency,

    -- Order metadata
    order_tracking_number,
    delivery_option_name,
    fulfillment_type,
    order_status,
    cancel_reason,

    -- Timestamps (UTC)
    cancel_time,
    order_delivery_time,
    paid_time,
    create_time,
    update_time,
    daton_batch_date,

    -- Metadata
    ds
from source_raw