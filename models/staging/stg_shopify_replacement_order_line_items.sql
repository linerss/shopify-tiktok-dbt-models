{{ config(materialized='table') }}

{# The replacement connector is adapted to the same contract as legacy staging. #}
{% set stores = var('shopify_replacement_stores', [
    {'store_id': 'store_a', 'source': 'shopify_replacement_store_a'},
    {'store_id': 'store_b', 'source': 'shopify_replacement_store_b'}
]) %}

{% for store in stores %}
select
    'shopify' as source_platform,
    'replacement' as connector,
    '{{ store.store_id }}' as store_id,
    {{ normalize_numeric_id('src.id') }} as order_id,
    {{ normalize_numeric_id('line.value:"id"') }} as line_item_id,
    {{ normalize_numeric_id('line.value:"product_id"') }} as product_id,
    {{ normalize_numeric_id('line.value:"variant_id"') }} as variant_id,
    line.value:"sku"::string as sku,
    line.value:"title"::string as product_title,
    line.value:"quantity"::number as quantity,
    line.value:"price"::number(18, 4) as unit_price,
    line.value:"total_discount"::number(18, 4) as line_discount,
    src.currency::string as currency,
    src.financial_status::string as financial_status,
    line.value:"fulfillment_status"::string as fulfillment_status,
    src.cancelled_at::timestamp_tz as cancelled_at,
    src.created_at::timestamp_tz as created_at,
    src.updated_at::timestamp_tz as updated_at,
    src._connector_emitted_at::timestamp_tz as source_loaded_at
from {{ source(store.source, 'orders') }} as src,
lateral flatten(input => src.line_items) as line

{% if not loop.last %}union all{% endif %}
{% endfor %}
