{{ config(materialized='table') }}

{# Only coarse geography is retained; customer PII and full addresses are omitted. #}
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
    {{ normalize_numeric_id('shipping.value:"id"') }} as shipping_line_id,
    shipping.value:"title"::string as shipping_title,
    shipping.value:"code"::string as shipping_code,
    shipping.value:"source"::string as shipping_source,
    shipping.value:"price"::number(18, 4) as shipping_price,
    shipping.value:"discounted_price"::number(18, 4) as discounted_shipping_price,
    src.currency::string as currency,
    src.shipping_address:"country_code"::string as shipping_country_code,
    src.shipping_address:"province_code"::string as shipping_province_code,
    src.created_at::timestamp_tz as created_at,
    src.updated_at::timestamp_tz as updated_at,
    src._connector_emitted_at::timestamp_tz as source_loaded_at
from {{ source(store.source, 'orders') }} as src,
lateral flatten(input => src.shipping_lines) as shipping

{% if not loop.last %}union all{% endif %}
{% endfor %}
