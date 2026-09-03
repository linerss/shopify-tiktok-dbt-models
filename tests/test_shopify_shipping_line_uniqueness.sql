select
    source_platform,
    store_id,
    order_id,
    shipping_line_id,
    count(*) as duplicate_count
from {{ ref('int_shopify_shipping_line_items') }}
group by source_platform, store_id, order_id, shipping_line_id
having count(*) > 1
