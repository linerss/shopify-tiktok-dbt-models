{{ config(materialized='table') }}

-- Grain: One row per order per row_type (sale or cancellation)
-- Sale rows: dated at create_time/paid_time with original shipping prices
-- Cancellation rows: dated at cancellation date with negated shipping prices

with brand_map as (
    {{ store_brand_mapping_sql() }}
),

earliest_snapshot as (
    -- Earliest snapshot per order by daton_batch_date to preserve original shipping fees
    select
        stg.source_platform,
        stg.brand,
        stg.order_id,
        -- Customer PII columns (email, name, phone, and street address) removed for open-source release.
        stg.order_tracking_number,
        stg.order_delivery_time,
        convert_timezone('UTC', 'America/Los_Angeles', stg.order_delivery_time) as order_delivery_time_pst,
        stg.order_status,
        stg.paid_time,
        convert_timezone('UTC', 'America/Los_Angeles', stg.paid_time) as paid_time_pst,
        stg.create_time,
        convert_timezone('UTC', 'America/Los_Angeles', stg.create_time) as create_time_pst,
        stg.delivery_option_name,
        stg.fulfillment_type,
        stg.cancel_reason,
        stg.cancel_time,
        convert_timezone('UTC', 'America/Los_Angeles', stg.cancel_time) as cancel_time_pst,
        stg.currency,
        stg.shipping_fee,
        stg.shipping_fee_cofunded_discount,
        stg.shipping_fee_platform_discount,
        stg.shipping_fee_seller_discount,
        stg.original_shipping_fee,
        stg.daton_batch_date,
        stg.update_time,
        convert_timezone('UTC', 'America/Los_Angeles', stg.update_time) as update_time_pst,
        stg.ds
    from {{ ref('stg_tiktok_shipping_line_items') }} stg
    qualify row_number() over (
        partition by order_id
        order by daton_batch_date asc
    ) = 1
),

latest_status as (
    -- Latest status per order by update_time
    select
        order_id,
        order_status,
        cancel_reason,
        cancel_time,
        convert_timezone('UTC', 'America/Los_Angeles', cancel_time) as cancel_time_pst,
        update_time,
        convert_timezone('UTC', 'America/Los_Angeles', update_time) as update_time_pst,
        daton_batch_date
    from {{ ref('stg_tiktok_shipping_line_items') }} stg
    qualify row_number() over (
        partition by order_id
        order by update_time desc, daton_batch_date desc
    ) = 1
),

sales as (
    -- Keep original shipping fees as a sale row
    select
        e.*,
        coalesce(e.paid_time, e.create_time, e.daton_batch_date) as event_date,
        convert_timezone('UTC', 'America/Los_Angeles', coalesce(e.paid_time, e.create_time, e.daton_batch_date)) as event_date_pst,
        'sale' as row_type
    from earliest_snapshot e
),

cancellations as (
    -- Emit a cancellation row dated at latest cancel time with negated values
    select
        -- identifiers
        e.source_platform,
        e.brand,
        e.order_id,
        e.order_tracking_number,

        -- order details
        e.order_delivery_time,
        e.order_delivery_time_pst,
        s.order_status,
        e.paid_time,
        e.paid_time_pst,
        e.create_time,
        e.create_time_pst,
        e.delivery_option_name,
        e.fulfillment_type,
        s.cancel_reason,
        s.cancel_time,
        s.cancel_time_pst,

        -- currency
        e.currency,

        -- shipping fees (negated to reverse original fees)
        -1 * abs(coalesce(e.shipping_fee, 0)) as shipping_fee,
        -1 * abs(coalesce(e.shipping_fee_cofunded_discount, 0)) as shipping_fee_cofunded_discount,
        -1 * abs(coalesce(e.shipping_fee_platform_discount, 0)) as shipping_fee_platform_discount,
        -1 * abs(coalesce(e.shipping_fee_seller_discount, 0)) as shipping_fee_seller_discount,
        -1 * abs(coalesce(e.original_shipping_fee, 0)) as original_shipping_fee,

        -- metadata
        e.daton_batch_date,
        s.update_time,
        s.update_time_pst,
        e.ds,

        -- event and type
        coalesce(s.cancel_time, s.update_time, e.paid_time) as event_date,
        convert_timezone('UTC', 'America/Los_Angeles', coalesce(s.cancel_time, s.update_time, e.paid_time)) as event_date_pst,
        'cancellation' as row_type
    from earliest_snapshot e
    join latest_status s
      on e.order_id = s.order_id
    where (
        s.cancel_reason is not null
        or s.cancel_time is not null
        or lower(coalesce(s.order_status,'')) like '%cancel%'
    )
),

combined_rows as (
    select * from sales
    union all
    select * from cancellations
)

select
    -- Keys
    concat_ws('_', c.source_platform, c.brand, c.order_id, 'shipping', c.row_type) as order_line_key,
    concat(c.source_platform, '_', upper(c.brand), '_', 'shipping') as product_key,

    -- Platform and brand
    c.source_platform,
    upper(c.brand) as brand,
    coalesce(bm.brand_full, upper(c.brand)) as brand_full,

    -- Order identifiers
    c.order_id,
    'shipping' as line_item_id,
    'shipping' as product_sku,
    'Shipping' as item_name,

    -- Order details
    c.order_delivery_time,
    c.order_delivery_time_pst,
    c.order_status,
    c.paid_time,
    c.paid_time_pst,
    c.create_time,
    c.create_time_pst,
    c.delivery_option_name,
    c.fulfillment_type,
    c.cancel_reason,
    c.cancel_time,
    c.cancel_time_pst,

    -- Product information
    upper(trim(c.currency)) as currency,

    -- Shipping fees
    c.original_shipping_fee as shipping_fee_original,
    c.shipping_fee_seller_discount as shipping_fee_seller_discount_total,
    (
        coalesce(c.original_shipping_fee, 0)
        - coalesce(c.shipping_fee_seller_discount, 0)
    ) as shipping_fee_after_seller_discount,
    c.shipping_fee_platform_discount as shipping_fee_platform_discount_total,

    -- Item status
    c.order_status as status,

    -- Metadata
    c.daton_batch_date,
    c.update_time,
    c.update_time_pst,
    c.ds,

    -- Event tracking
    c.event_date,
    c.event_date_pst,
    c.row_type,

    -- dbt metadata
    current_timestamp() as dbt_last_updated,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as dbt_run_id

from combined_rows as c
left join brand_map bm
  on bm.brand = upper(c.brand)
