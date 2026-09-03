with staged_ids as (
    select store_id, order_id, line_item_id
    from {{ ref('stg_shopify_legacy_order_line_items') }}

    union all

    select store_id, order_id, line_item_id
    from {{ ref('stg_shopify_replacement_order_line_items') }}
)

select store_id, order_id, line_item_id
from staged_ids
where line_item_id is not null
  and (
      line_item_id like '%.%'
      or regexp_like(line_item_id, '[eE]')
  )
