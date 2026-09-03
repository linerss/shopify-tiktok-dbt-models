with staged_ids as (
    select store_id, order_id, shipping_line_id
    from {{ ref('stg_shopify_legacy_shipping_line_items') }}

    union all

    select store_id, order_id, shipping_line_id
    from {{ ref('stg_shopify_replacement_shipping_line_items') }}
)

select store_id, order_id, shipping_line_id
from staged_ids
where shipping_line_id is not null
  and (
      shipping_line_id like '%.%'
      or regexp_like(shipping_line_id, '[eE]')
  )
