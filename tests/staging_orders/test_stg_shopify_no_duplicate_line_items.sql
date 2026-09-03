-- Detect duplicate Shopify line items caused by mixed integer/decimal formats
-- DISABLED: Staging tables use incremental append and will have duplicates by design
-- Daton sends updates for old orders, creating both old format and new format entries
-- Deduplication happens in intermediate layer (see test_int_shopify_no_duplicate_line_items)
{{ config(enabled=false) }}

with normalized as (
    select
        brand,
        order_id,
        try_to_number(line_item_id) as line_item_id_num,
        line_item_id
    from {{ ref('stg_shopify_order_line_items') }}
    where line_item_id is not null
        and ds >= '<CUTOVER_DATE>'  -- Only check data after normalization was added to staging model
),

duplicate_formats as (
    select
        brand,
        order_id,
        line_item_id_num,
        count(distinct line_item_id) as distinct_formats,
        listagg(distinct line_item_id, ', ') within group (order by line_item_id) as format_examples
    from normalized
    where line_item_id_num is not null
    group by 1, 2, 3
    having count(distinct line_item_id) > 1
)

select
    brand,
    order_id,
    line_item_id_num,
    distinct_formats,
    format_examples,
    'Line item ID appears in multiple formats for the same order_id (possible Daton decimal format issue).' as error_message
from duplicate_formats
order by brand, order_id, line_item_id_num
