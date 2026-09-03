select
    source_platform,
    store_id,
    refund_id,
    refund_row_id,
    count(*) as duplicate_count
from {{ ref('int_shopify_refund_line_items') }}
where refund_type = 'shipping_refund'
group by source_platform, store_id, refund_id, refund_row_id
having count(*) > 1
