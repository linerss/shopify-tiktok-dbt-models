{{ config(
  materialized='table'
) }}

-- Grain: One row per order line (order_id, line_item_id) per row_type (sale or cancellation)
-- Sale rows: dated at create_time/paid_time with original prices
-- Cancellation rows: dated at cancellation date with negated prices
--
-- IMPORTANT: TikTok's cancel_reason field is filled in for ALL customer issues including:
--   - Actual refunds (money back to customer) → CREATE cancellation row
--   - Replacements (no money back, customer gets new item) → NO cancellation row
--   - Exchanges (no money back, customer gets different item) → NO cancellation row
--
-- We use stg_tiktok_returns to determine if money actually went back to the customer.
-- Only return_type = 'REFUND' or 'RETURN_AND_REFUND' should create cancellation rows.
-- Shopify refunds are in a separate table (stg/int_shopify_refund_line_items).

with brand_map as (
    {{ store_brand_mapping_sql() }}
),

order_returns as (
    -- Get return type per order to determine if money was actually refunded
    -- Only REFUND and RETURN_AND_REFUND mean money went back to customer
    -- REPLACEMENT and EXCHANGE do NOT refund money - we keep the original sale
    select
        brand,
        order_id,
        return_type,
        is_actual_refund,
        refund_total,
        create_time as return_created_at,  -- stable date for when refund was initiated
        row_number() over (
            partition by brand, order_id
            order by update_time desc, daton_batch_runtime_ts desc
        ) as rn
    from {{ ref('stg_tiktok_returns') }}
    qualify rn = 1
),

earliest_snapshot as (
    -- Earliest snapshot per order/line_item by daton_batch_date to preserve original revenue
    select
        stg.source_platform,
        stg.brand,
        stg.order_id,
        stg.line_item_id,
        -- Customer PII columns (email, name, phone, and street address) removed for open-source release.
        stg.order_tracking_number,
        stg.order_delivery_time,
        stg.is_exchange_order,
        stg.order_status,
        stg.paid_time,
        convert_timezone('UTC', 'America/Los_Angeles', stg.paid_time) as paid_time_pst,
        stg.create_time,
        convert_timezone('UTC', 'America/Los_Angeles', stg.create_time) as create_time_pst,
        stg.delivery_option_name,
        stg.fulfillment_type,
        stg.auto_combine_group_id,
        stg.buyer_message,
        stg.cancel_order_sla_time,
        stg.cancel_reason,
        stg.cancel_time,
        convert_timezone('UTC', 'America/Los_Angeles', stg.cancel_time) as cancel_time_pst,
        stg.cancellation_initiator,
        stg.collection_due_time,
        stg.collection_time,
        stg.cpf,
        stg.delivery_due_time,
        stg.delivery_option_id,
        stg.delivery_option_required_delivery_time,
        stg.delivery_sla_time,
        stg.delivery_type,
        stg.fast_delivery_program,
        stg.fast_dispatch_sla_time,
        stg.has_updated_recipient_address,
        stg.is_buyer_request_cancel,
        stg.is_cod,
        stg.is_on_hold_order,
        stg.is_replacement_order,
        stg.is_sample_order,
        stg.need_upload_invoice,
        stg.payment_method_name,
        stg.pick_up_cut_off_time,
        stg.replaced_order_id,
        stg.request_cancel_time,
        convert_timezone('UTC', 'America/Los_Angeles', stg.request_cancel_time) as request_cancel_time_pst,
        stg.rts_sla_time,
        stg.rts_time,
        stg.seller_note,
        stg.shipping_due_time,
        stg.shipping_provider,
        stg.shipping_provider_id,
        stg.shipping_type,
        stg.split_or_combine_tag,
        stg.tts_sla_time,
        stg.user_id,
        stg.warehouse_id,
        stg.currency,
        stg.product_id,
        stg.product_name,
        stg.sku_id,
        stg.sku_name,
        stg.sku_image,
        stg.seller_sku,
        stg.sale_price,
        stg.original_price,
        stg.seller_discount,
        stg.platform_discount,
        stg.item_tax_amount,
        stg.package_status,
        stg.package_id,
        stg.rts_time_ts,
        stg.daton_batch_date,
        stg.update_time,
        convert_timezone('UTC', 'America/Los_Angeles', stg.update_time) as update_time_pst,
        stg.ds
    from (
        select
            stg.*,
            row_number() over (
                partition by order_id, line_item_id
                -- Pick earliest batch, but prefer positive prices on ties
                order by daton_batch_date asc, original_price desc
            ) as rn_first
        from {{ ref('stg_tiktok_order_line_items') }} stg
    ) stg
    qualify rn_first = 1
),

latest_status as (
    -- Latest status per order/line_item by update_time
    select
        order_id,
        line_item_id,
        order_status,
        cancel_reason,
        cancel_time,
        convert_timezone('UTC', 'America/Los_Angeles', cancel_time) as cancel_time_pst,
        package_status,
        update_time,
        convert_timezone('UTC', 'America/Los_Angeles', update_time) as update_time_pst,
        daton_batch_date,
        row_number() over (
            partition by order_id, line_item_id
            order by update_time desc, daton_batch_date desc
        ) as rn_status
    from {{ ref('stg_tiktok_order_line_items') }}
    qualify rn_status = 1
),

sales as (
    -- Keep original revenue as a sale row
    select
        e.*,
        coalesce(e.paid_time, e.create_time, e.daton_batch_date) as event_date,
        convert_timezone('UTC', 'America/Los_Angeles', coalesce(e.paid_time, e.create_time, e.daton_batch_date)) as event_date_pst,
        'sale' as row_type
    from earliest_snapshot e
),

cancellations as (
    -- Emit a cancellation row dated at latest cancel time with negated values
    -- ONLY for actual refunds, NOT for replacements/exchanges
    select
        -- identifiers
        e.source_platform,
        e.brand,
        e.order_id,
        e.line_item_id,
        e.order_tracking_number,

        -- order details
        e.order_delivery_time,
        e.is_exchange_order,
        s.order_status,
        e.paid_time,
        e.paid_time_pst,
        e.create_time,
        e.create_time_pst,
        e.delivery_option_name,
        e.fulfillment_type,

        -- additional order fields
        e.auto_combine_group_id,
        e.buyer_message,
        e.cancel_order_sla_time,
        s.cancel_reason,
        s.cancel_time,
        s.cancel_time_pst,
        e.cancellation_initiator,
        e.collection_due_time,
        e.collection_time,
        e.cpf,
        e.delivery_due_time,
        e.delivery_option_id,
        e.delivery_option_required_delivery_time,
        e.delivery_sla_time,
        e.delivery_type,
        e.fast_delivery_program,
        e.fast_dispatch_sla_time,
        e.has_updated_recipient_address,
        e.is_buyer_request_cancel,
        e.is_cod,
        e.is_on_hold_order,
        e.is_replacement_order,
        e.is_sample_order,
        e.need_upload_invoice,
        e.payment_method_name,
        e.pick_up_cut_off_time,
        e.replaced_order_id,
        e.request_cancel_time,
        e.request_cancel_time_pst,
        e.rts_sla_time,
        e.rts_time,
        e.seller_note,
        e.shipping_due_time,
        e.shipping_provider,
        e.shipping_provider_id,
        e.shipping_type,
        e.split_or_combine_tag,
        e.tts_sla_time,
        e.user_id,
        e.warehouse_id,

        -- product info
        e.currency,
        e.product_id,
        e.product_name,
        e.sku_id,
        e.sku_name,
        e.sku_image,
        e.seller_sku,

        -- pricing (negated to reverse original revenue)
        -1 * abs(coalesce(e.sale_price, 0)) as sale_price,
        -1 * abs(coalesce(e.original_price, 0)) as original_price,
        -1 * abs(coalesce(e.seller_discount, 0)) as seller_discount,
        -1 * abs(coalesce(e.platform_discount, 0)) as platform_discount,
        -1 * abs(coalesce(e.item_tax_amount, 0)) as item_tax_amount,

        -- item status
        s.package_status,
        e.package_id,
        e.rts_time_ts,

        -- metadata
        e.daton_batch_date,
        s.update_time,
        s.update_time_pst,
        e.ds,

        -- event and type
        -- Use return_created_at for refunds (stable), fall back to cancel_time/update_time for other cancellations
        coalesce(r.return_created_at, s.cancel_time, s.update_time, e.paid_time) as event_date,
        convert_timezone('UTC', 'America/Los_Angeles', coalesce(r.return_created_at, s.cancel_time, s.update_time, e.paid_time)) as event_date_pst,
        'cancellation' as row_type
    from earliest_snapshot e
    join latest_status s
      on e.order_id = s.order_id
     and e.line_item_id = s.line_item_id
    left join order_returns r
      on e.brand = r.brand
     and e.order_id = r.order_id
    where (
        -- Case 1: Order is in returns table with actual refund (REFUND or RETURN_AND_REFUND)
        r.is_actual_refund = true
        -- Case 2: Order was cancelled (not in returns table) - cancelled before shipping
        or (
            r.order_id is null
            and (
                lower(coalesce(s.order_status,'')) like '%cancel%'
                or lower(coalesce(s.package_status,'')) like '%cancel%'
            )
        )
    )
    -- Exclude replacements and exchanges even if cancel_reason is set
    -- These have r.is_actual_refund = false, so they won't match Case 1
    -- And they ARE in returns table, so they won't match Case 2
),

combined_rows as (
    select * from sales
    union all
    select * from cancellations
)

select
    -- Keys (for downstream compatibility)
    concat_ws('_', c.source_platform, c.brand, c.order_id, c.line_item_id, c.row_type) as order_line_key,
    concat(c.source_platform, '_', c.brand, '_', case when c.seller_sku = '' then c.product_id else c.seller_sku end) as product_key,

    -- Platform and brand
    c.source_platform,
    c.brand,
    coalesce(bm.brand_full, c.brand) as brand_full,

    -- Order identifiers
    c.order_id,
    c.line_item_id,
    c.order_tracking_number,

    -- Order details
    c.order_delivery_time,
    c.is_exchange_order,
    c.order_status,
    c.paid_time,
    c.paid_time_pst,
    c.create_time,
    c.create_time_pst,
    c.delivery_option_name,
    c.fulfillment_type,

    -- Additional order fields
    c.auto_combine_group_id,
    c.buyer_message,
    c.cancel_order_sla_time,
    c.cancel_reason,
    c.cancel_time,
    c.cancel_time_pst,
    c.cancellation_initiator,
    c.collection_due_time,
    c.collection_time,
    c.cpf,
    c.delivery_due_time,
    c.delivery_option_id,
    c.delivery_option_required_delivery_time,
    c.delivery_sla_time,
    c.delivery_type,
    c.fast_delivery_program,
    c.fast_dispatch_sla_time,
    c.has_updated_recipient_address,
    c.is_buyer_request_cancel,
    c.is_cod,
    c.is_on_hold_order,
    c.is_replacement_order,
    c.is_sample_order,
    c.need_upload_invoice,
    c.payment_method_name,
    c.pick_up_cut_off_time,
    c.replaced_order_id,
    c.request_cancel_time,
    c.request_cancel_time_pst,
    c.rts_sla_time,
    c.rts_time,
    c.seller_note,
    c.shipping_due_time,
    c.shipping_provider,
    c.shipping_provider_id,
    c.shipping_type,
    c.split_or_combine_tag,
    c.tts_sla_time,
    c.user_id,
    c.warehouse_id,

    -- Product information
    c.currency,
    c.product_id,
    c.product_name,
    coalesce(c.sku_name, c.product_name) as item_name,  -- alias for downstream
    case when c.seller_sku = '' then c.product_id else c.seller_sku end as product_sku,  -- alias for downstream
    c.sku_id,
    c.sku_name,
    c.sku_image,
    c.seller_sku,

    -- Pricing
    c.sale_price,
    c.original_price,
    c.original_price as list_price,  -- alias for downstream
    c.original_price as list_price_per_unit,  -- alias for downstream
    c.seller_discount,
    c.seller_discount as seller_discount_per_unit,  -- alias for downstream
    c.platform_discount,
    c.item_tax_amount,

    -- Units (TikTok line items are always quantity 1, negative for cancellations)
    case when c.row_type = 'cancellation' then -1 else 1 end as units,
    case when c.row_type = 'cancellation' then -1 else 1 end as units_count,

    -- Item status
    c.order_status as status,  -- alias for downstream
    c.package_status,
    c.package_id,
    c.rts_time_ts,

    -- Metadata
    c.daton_batch_date,
    c.update_time,
    c.update_time_pst,
    c.ds,

    -- Event tracking
    c.event_date,
    c.event_date_pst,
    c.row_type,

    -- Calculated fields
    -- sales_amount = original_price - seller_discount (TikTok doesn't have shipping in line items)
    (
        coalesce(c.original_price, 0)
        - coalesce(c.seller_discount, 0)
    ) as sales_amount,
    (
        coalesce(c.original_price, 0)
        - coalesce(c.seller_discount, 0)
    ) as sales_amount_per_unit,  -- same as sales_amount since quantity is always 1

    -- dbt metadata
    current_timestamp() as dbt_last_updated,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as dbt_run_id

from combined_rows as c
left join brand_map bm
  on bm.brand = c.brand
