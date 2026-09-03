{{ config(materialized='table') }}

{% set migrated_stores = var('shopify_migrated_stores', ['store_a']) %}

with connector_union as (
    select
        source_platform,
        connector,
        store_id,
        order_id,
        shipping_line_id,
        shipping_title,
        shipping_code,
        shipping_source,
        shipping_price,
        discounted_shipping_price,
        currency,
        shipping_country_code,
        shipping_province_code,
        created_at,
        updated_at,
        source_loaded_at
    from {{ ref('stg_shopify_legacy_shipping_line_items') }}
    {% if migrated_stores | length > 0 %}
    where store_id not in (
        {% for store_id in migrated_stores %}'{{ store_id }}'{% if not loop.last %}, {% endif %}{% endfor %}
    )
    {% endif %}

    union all

    select
        source_platform,
        connector,
        store_id,
        order_id,
        shipping_line_id,
        shipping_title,
        shipping_code,
        shipping_source,
        shipping_price,
        discounted_shipping_price,
        currency,
        shipping_country_code,
        shipping_province_code,
        created_at,
        updated_at,
        source_loaded_at
    from {{ ref('stg_shopify_replacement_shipping_line_items') }}
    {% if migrated_stores | length > 0 %}
    where store_id in (
        {% for store_id in migrated_stores %}'{{ store_id }}'{% if not loop.last %}, {% endif %}{% endfor %}
    )
    {% else %}
    where 1 = 0
    {% endif %}
),

normalized as (
    select
        source_platform,
        connector,
        store_id,
        {{ normalize_numeric_id('order_id') }} as order_id,
        {{ normalize_numeric_id('shipping_line_id') }} as shipping_line_id,
        shipping_title,
        shipping_code,
        shipping_source,
        shipping_price,
        discounted_shipping_price,
        currency,
        shipping_country_code,
        shipping_province_code,
        created_at,
        updated_at,
        source_loaded_at
    from connector_union
)

select
    concat_ws('|', source_platform, store_id, order_id, shipping_line_id) as shipping_line_key,
    source_platform,
    connector,
    store_id,
    order_id,
    shipping_line_id,
    shipping_title,
    shipping_code,
    shipping_source,
    shipping_price,
    discounted_shipping_price,
    currency,
    shipping_country_code,
    shipping_province_code,
    created_at,
    updated_at,
    source_loaded_at
from normalized
qualify row_number() over (
    partition by source_platform, store_id, order_id, shipping_line_id
    order by updated_at desc nulls last, source_loaded_at desc
) = 1
