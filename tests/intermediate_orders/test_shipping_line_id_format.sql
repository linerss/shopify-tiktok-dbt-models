-- Validate shipping_line_id normalization to integer-like strings
{{ config(severity='warn') }}

select
    brand,
    order_id,
    shipping_line_id,
    'shipping_line_id contains decimal or scientific notation; expected integer-like string.' as error_message
from {{ ref('stg_shopify_shipping_line_items') }}
where shipping_line_id is not null
  and (shipping_line_id like '%.%' or regexp_like(shipping_line_id, '[eE]'))
order by brand, order_id
