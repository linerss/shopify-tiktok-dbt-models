{{ config(materialized='table') }}

{#
  This model publishes shipping sales only. Product-return payloads do not prove
  that shipping was refunded, so no negative shipping event is inferred. Add an
  explicit shipping-refund source and amount before modeling shipping reversals.
#}
with earliest_shipping_snapshot as (
    select
        source_platform,
        store_id,
        {{ normalize_numeric_id('order_id') }} as order_id,
        shipping_line_id,
        shipping_fee,
        platform_shipping_discount,
        seller_shipping_discount,
        currency,
        shipping_country_code,
        shipping_province_code,
        order_status,
        created_at,
        source_loaded_at
    from {{ ref('stg_tiktok_shipping_line_items') }}
    qualify row_number() over (
        partition by source_platform, store_id, order_id, shipping_line_id
        order by source_loaded_at asc
    ) = 1
),

latest_shipping_snapshot as (
    select
        source_platform,
        store_id,
        {{ normalize_numeric_id('order_id') }} as order_id,
        shipping_line_id,
        order_status as latest_order_status,
        updated_at,
        source_loaded_at
    from {{ ref('stg_tiktok_shipping_line_items') }}
    qualify row_number() over (
        partition by source_platform, store_id, order_id, shipping_line_id
        order by updated_at desc nulls last, source_loaded_at desc
    ) = 1
)

select
    concat_ws('|', sale.source_platform, sale.store_id, sale.order_id, sale.shipping_line_id, 'sale') as shipping_event_key,
    'sale' as row_type,
    sale.source_platform,
    sale.store_id,
    sale.order_id,
    sale.shipping_line_id,
    abs(sale.shipping_fee - coalesce(sale.platform_shipping_discount, 0) - coalesce(sale.seller_shipping_discount, 0)) as net_shipping_amount,
    sale.currency,
    sale.shipping_country_code,
    sale.shipping_province_code,
    sale.order_status as original_order_status,
    status.latest_order_status,
    sale.created_at,
    status.updated_at,
    status.source_loaded_at
from earliest_shipping_snapshot as sale
inner join latest_shipping_snapshot as status
    on sale.source_platform = status.source_platform
   and sale.store_id = status.store_id
   and sale.order_id = status.order_id
   and sale.shipping_line_id = status.shipping_line_id
