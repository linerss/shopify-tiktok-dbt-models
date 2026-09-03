-- Verify that the same line item does not appear in multiple numeric formats across batches
{{ config(severity='warn') }}

with base as (
    select
        'stg_shopify_order_line_items' as model_name,
        brand,
        order_id,
        try_to_number(line_item_id) as normalized_id,
        line_item_id as raw_id
    from {{ ref('stg_shopify_order_line_items') }}
    where line_item_id is not null

    union all

    select
        'stg_shopify_shipping_line_items' as model_name,
        brand,
        order_id,
        try_to_number(shipping_line_id) as normalized_id,
        shipping_line_id as raw_id
    from {{ ref('stg_shopify_shipping_line_items') }}
    where shipping_line_id is not null
),

duplicate_formats as (
    select
        model_name,
        brand,
        order_id,
        normalized_id,
        count(distinct raw_id) as distinct_formats,
        listagg(distinct raw_id, ', ') within group (order by raw_id) as format_examples
    from base
    where normalized_id is not null
    group by 1, 2, 3, 4
    having count(distinct raw_id) > 1
)

select
    model_name,
    brand,
    order_id,
    normalized_id,
    distinct_formats,
    format_examples,
    'Line item ID has multiple raw formats for the same normalized numeric ID.' as error_message
from duplicate_formats
order by model_name, brand, order_id, normalized_id
