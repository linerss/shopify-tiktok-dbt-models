select
    order_line_event_key,
    count(*) as duplicate_count
from {{ ref('int_tiktok_order_line_items') }}
group by order_line_event_key
having count(*) > 1
