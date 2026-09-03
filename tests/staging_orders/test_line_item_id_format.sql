-- Validate that line item identifiers are normalized to integer-like strings
{{ config(severity='warn') }}

with ids as (
    select 'stg_shopify_order_line_items' as model_name, 'line_item_id' as id_field, line_item_id as id_value
    from {{ ref('stg_shopify_order_line_items') }}

    union all

    select 'stg_shopify_shipping_line_items' as model_name, 'shipping_line_id' as id_field, shipping_line_id as id_value
    from {{ ref('stg_shopify_shipping_line_items') }}

    union all

    select 'stg_tiktok_order_line_items' as model_name, 'line_item_id' as id_field, line_item_id as id_value
    from {{ ref('stg_tiktok_order_line_items') }}

    union all

    select 'stg_shopify_refund_line_items' as model_name, 'refund_line_item_id' as id_field, refund_line_item_id as id_value
    from {{ ref('stg_shopify_refund_line_items') }}

    union all

    select 'stg_shopify_refund_line_items' as model_name, 'a_linked_refund_id' as id_field, a_linked_refund_id as id_value
    from {{ ref('stg_shopify_refund_line_items') }}
)

select
    model_name,
    id_field,
    id_value,
    'Identifier contains decimal or scientific notation; expected integer-like string.' as error_message
from ids
where id_value is not null
  and (id_value like '%.%' or regexp_like(id_value, '[eE]'))
order by model_name, id_field
