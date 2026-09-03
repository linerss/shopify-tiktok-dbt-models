select
    source_platform,
    store_id,
    order_id,
    line_item_id,
    count(*) as duplicate_count
from {{ ref('int_shopify_order_line_items') }}
group by source_platform, store_id, order_id, line_item_id
having count(*) > 1
