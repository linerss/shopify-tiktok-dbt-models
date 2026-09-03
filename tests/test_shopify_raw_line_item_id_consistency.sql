{{ config(severity='warn') }}

{# Diagnostic: equivalent numeric IDs should not arrive in multiple string forms. #}
{% set stores = var('shopify_legacy_stores', [
    {'store_id': 'store_a', 'table': 'store_a_orders'},
    {'store_id': 'store_b', 'table': 'store_b_orders'}
]) %}

with raw_ids as (
    {% for store in stores %}
    select
        '{{ store.store_id }}' as store_id,
        {{ normalize_numeric_id('src.value:"id"') }} as order_id,
        line.value:"id"::string as raw_line_item_id,
        {{ normalize_numeric_id('line.value:"id"') }} as normalized_line_item_id
    from {{ source('shopify_legacy', store.table) }} as src,
    lateral flatten(input => try_parse_json(src.value:"line_items"::string)) as line
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
)

select
    store_id,
    order_id,
    normalized_line_item_id,
    count(distinct raw_line_item_id) as representation_count
from raw_ids
group by store_id, order_id, normalized_line_item_id
having count(distinct raw_line_item_id) > 1
