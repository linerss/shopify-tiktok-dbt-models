with expected_line_refunds as (
    select
        source_platform,
        store_id,
        order_id,
        line_item_id,
        sum(refund_amount) as expected_refund_amount
    from {{ ref('int_tiktok_returns') }}
    where reverses_revenue
      and line_item_id is not null
      and coalesce(refund_amount, 0) > 0
    group by source_platform, store_id, order_id, line_item_id
),

actual_line_refunds as (
    select
        source_platform,
        store_id,
        order_id,
        line_item_id,
        abs(sum(net_line_amount)) as actual_refund_amount
    from {{ ref('int_tiktok_order_line_items') }}
    where row_type = 'cancellation'
    group by source_platform, store_id, order_id, line_item_id
)

select
    expected.source_platform,
    expected.store_id,
    expected.order_id,
    expected.line_item_id,
    expected.expected_refund_amount,
    actual.actual_refund_amount
from expected_line_refunds as expected
left join actual_line_refunds as actual
    on expected.source_platform = actual.source_platform
   and expected.store_id = actual.store_id
   and expected.order_id = actual.order_id
   and expected.line_item_id = actual.line_item_id
where actual.actual_refund_amount is null
   or abs(expected.expected_refund_amount - actual.actual_refund_amount) > 0.01
