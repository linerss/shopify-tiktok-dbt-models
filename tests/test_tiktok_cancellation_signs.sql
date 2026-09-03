select
    order_line_event_key,
    row_type,
    quantity,
    net_line_amount
from {{ ref('int_tiktok_order_line_items') }}
where (row_type = 'sale' and (quantity < 0 or net_line_amount < 0))
   or (row_type = 'cancellation' and (quantity > 0 or net_line_amount > 0))
