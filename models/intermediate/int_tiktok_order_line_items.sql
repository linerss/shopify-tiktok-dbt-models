{{ config(materialized='table') }}

{#
  The first snapshot preserves original sale economics. The latest snapshot
  supplies current operational status. Only completed refunds reverse revenue.
  Line-level refunds use the connector's refund amount, so partial refunds do
  not reverse the full line. Order-level refunds remain explicit unallocated
  events instead of being silently dropped or spread across products.
#}
with earliest_financial_snapshot as (
    select
        source_platform,
        store_id,
        {{ normalize_numeric_id('order_id') }} as order_id,
        {{ normalize_numeric_id('line_item_id') }} as line_item_id,
        {{ normalize_numeric_id('product_id') }} as product_id,
        sku,
        product_title,
        quantity,
        original_unit_price,
        sale_unit_price,
        platform_discount,
        seller_discount,
        abs((sale_unit_price * quantity) - coalesce(platform_discount, 0) - coalesce(seller_discount, 0)) as original_net_line_amount,
        currency,
        order_status,
        created_at,
        paid_at,
        updated_at,
        source_loaded_at
    from {{ ref('stg_tiktok_order_line_items') }}
    qualify row_number() over (
        partition by source_platform, store_id, order_id, line_item_id
        order by source_loaded_at asc
    ) = 1
),

latest_operational_snapshot as (
    select
        source_platform,
        store_id,
        {{ normalize_numeric_id('order_id') }} as order_id,
        {{ normalize_numeric_id('line_item_id') }} as line_item_id,
        order_status as latest_order_status,
        updated_at as latest_updated_at,
        source_loaded_at as latest_source_loaded_at
    from {{ ref('stg_tiktok_order_line_items') }}
    qualify row_number() over (
        partition by source_platform, store_id, order_id, line_item_id
        order by updated_at desc nulls last, source_loaded_at desc
    ) = 1
),

line_refunds as (
    select
        source_platform,
        store_id,
        order_id,
        line_item_id,
        sum(refund_amount) as refund_amount,
        max(updated_at) as return_updated_at,
        max(source_loaded_at) as return_source_loaded_at
    from {{ ref('int_tiktok_returns') }}
    where reverses_revenue
      and line_item_id is not null
      and coalesce(refund_amount, 0) > 0
    group by source_platform, store_id, order_id, line_item_id
),

order_refunds as (
    select
        source_platform,
        store_id,
        order_id,
        return_id,
        refund_amount,
        currency,
        updated_at as return_updated_at,
        source_loaded_at as return_source_loaded_at
    from {{ ref('int_tiktok_returns') }}
    where reverses_revenue
      and line_item_id is null
      and coalesce(refund_amount, 0) > 0
),

base as (
    select
        sale.source_platform,
        sale.store_id,
        sale.order_id,
        sale.line_item_id,
        sale.product_id,
        sale.sku,
        sale.product_title,
        sale.quantity,
        sale.original_net_line_amount,
        sale.currency,
        sale.order_status as original_order_status,
        status.latest_order_status,
        sale.created_at,
        sale.paid_at,
        status.latest_updated_at as updated_at,
        status.latest_source_loaded_at as source_loaded_at,
        returned.refund_amount,
        returned.return_updated_at,
        returned.return_source_loaded_at
    from earliest_financial_snapshot as sale
    inner join latest_operational_snapshot as status
        on sale.source_platform = status.source_platform
       and sale.store_id = status.store_id
       and sale.order_id = status.order_id
       and sale.line_item_id = status.line_item_id
    left join line_refunds as returned
        on sale.source_platform = returned.source_platform
       and sale.store_id = returned.store_id
       and sale.order_id = returned.order_id
       and sale.line_item_id = returned.line_item_id
),

order_context as (
    select
        source_platform,
        store_id,
        order_id,
        min(original_order_status) as original_order_status,
        min(latest_order_status) as latest_order_status,
        min(created_at) as created_at,
        min(paid_at) as paid_at
    from base
    group by source_platform, store_id, order_id
),

sales as (
    select
        concat_ws('|', source_platform, store_id, order_id, line_item_id, 'sale') as order_line_event_key,
        'sale' as row_type,
        source_platform,
        store_id,
        order_id,
        line_item_id,
        product_id,
        sku,
        product_title,
        quantity,
        original_net_line_amount as net_line_amount,
        currency,
        original_order_status,
        latest_order_status,
        created_at,
        paid_at,
        updated_at,
        source_loaded_at
    from base
),

line_refund_events as (
    select
        concat_ws('|', source_platform, store_id, order_id, line_item_id, 'cancellation') as order_line_event_key,
        'cancellation' as row_type,
        source_platform,
        store_id,
        order_id,
        line_item_id,
        product_id,
        sku,
        product_title,
        case
            when refund_amount >= original_net_line_amount then -abs(quantity)
            else null
        end as quantity,
        -abs(refund_amount) as net_line_amount,
        currency,
        original_order_status,
        latest_order_status,
        created_at,
        paid_at,
        return_updated_at as updated_at,
        return_source_loaded_at as source_loaded_at
    from base
    where refund_amount > 0
),

unallocated_order_refund_events as (
    select
        concat_ws('|', refund.source_platform, refund.store_id, refund.order_id, refund.return_id, 'unallocated_refund') as order_line_event_key,
        'cancellation' as row_type,
        refund.source_platform,
        refund.store_id,
        refund.order_id,
        concat('order_refund_', refund.return_id) as line_item_id,
        null::string as product_id,
        null::string as sku,
        'Unallocated order refund' as product_title,
        null::number as quantity,
        -abs(refund.refund_amount) as net_line_amount,
        refund.currency,
        context.original_order_status,
        context.latest_order_status,
        context.created_at,
        context.paid_at,
        refund.return_updated_at as updated_at,
        refund.return_source_loaded_at as source_loaded_at
    from order_refunds as refund
    left join order_context as context
        on refund.source_platform = context.source_platform
       and refund.store_id = context.store_id
       and refund.order_id = context.order_id
),

ledger as (
    select order_line_event_key, row_type, source_platform, store_id, order_id, line_item_id, product_id, sku, product_title, quantity, net_line_amount, currency, original_order_status, latest_order_status, created_at, paid_at, updated_at, source_loaded_at
    from sales
    union all
    select order_line_event_key, row_type, source_platform, store_id, order_id, line_item_id, product_id, sku, product_title, quantity, net_line_amount, currency, original_order_status, latest_order_status, created_at, paid_at, updated_at, source_loaded_at
    from line_refund_events
    union all
    select order_line_event_key, row_type, source_platform, store_id, order_id, line_item_id, product_id, sku, product_title, quantity, net_line_amount, currency, original_order_status, latest_order_status, created_at, paid_at, updated_at, source_loaded_at
    from unallocated_order_refund_events
)

select
    order_line_event_key,
    row_type,
    source_platform,
    store_id,
    order_id,
    line_item_id,
    product_id,
    sku,
    product_title,
    quantity,
    net_line_amount,
    currency,
    original_order_status,
    latest_order_status,
    created_at,
    paid_at,
    updated_at,
    source_loaded_at
from ledger
