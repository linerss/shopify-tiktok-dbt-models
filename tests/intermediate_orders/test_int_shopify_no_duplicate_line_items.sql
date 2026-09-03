-- Ensure intermediate Shopify line items are unique on natural keys
{{ config(severity='error') }}

with duplicate_check as (
    select
        source_platform,
        brand,
        order_id,
        line_item_id,
        row_type,
        count(*) as row_count
    from {{ ref('int_shopify_order_line_items') }}
    group by 1, 2, 3, 4, 5
    having count(*) > 1
)

select
    source_platform,
    brand,
    order_id,
    line_item_id,
    row_type,
    row_count,
    'Duplicate line item in int_shopify_order_line_items for natural key.' as error_message
from duplicate_check
order by brand, order_id, line_item_id
