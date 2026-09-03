{{ config(materialized='table') }}

{% set migrated_stores = var('shopify_migrated_stores', ['store_a']) %}

with connector_union as (
    select
        source_platform,
        connector,
        store_id,
        order_id,
        line_item_id,
        product_id,
        variant_id,
        sku,
        product_title,
        quantity,
        unit_price,
        line_discount,
        currency,
        financial_status,
        fulfillment_status,
        cancelled_at,
        created_at,
        updated_at,
        source_loaded_at
    from {{ ref('stg_shopify_legacy_order_line_items') }}
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
        line_item_id,
        product_id,
        variant_id,
        sku,
        product_title,
        quantity,
        unit_price,
        line_discount,
        currency,
        financial_status,
        fulfillment_status,
        cancelled_at,
        created_at,
        updated_at,
        source_loaded_at
    from {{ ref('stg_shopify_replacement_order_line_items') }}
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
        {{ normalize_numeric_id('line_item_id') }} as line_item_id,
        {{ normalize_numeric_id('product_id') }} as product_id,
        {{ normalize_numeric_id('variant_id') }} as variant_id,
        sku,
        product_title,
        quantity,
        unit_price,
        line_discount,
        quantity * unit_price as gross_line_amount,
        (quantity * unit_price) - coalesce(line_discount, 0) as net_line_amount,
        currency,
        financial_status,
        fulfillment_status,
        cancelled_at,
        created_at,
        updated_at,
        source_loaded_at
    from connector_union
),

current_snapshot as (
    select
        source_platform,
        connector,
        store_id,
        order_id,
        line_item_id,
        product_id,
        variant_id,
        sku,
        product_title,
        quantity,
        unit_price,
        line_discount,
        gross_line_amount,
        net_line_amount,
        currency,
        financial_status,
        fulfillment_status,
        cancelled_at,
        created_at,
        updated_at,
        source_loaded_at
    from normalized
    qualify row_number() over (
        partition by source_platform, store_id, order_id, line_item_id
        order by updated_at desc nulls last, source_loaded_at desc
    ) = 1
)

select
    concat_ws('|', source_platform, store_id, order_id, line_item_id) as order_line_key,
    source_platform,
    connector,
    store_id,
    order_id,
    line_item_id,
    product_id,
    variant_id,
    sku,
    product_title,
    quantity,
    unit_price,
    line_discount,
    gross_line_amount,
    net_line_amount,
    currency,
    financial_status,
    fulfillment_status,
    cancelled_at,
    created_at,
    updated_at,
    source_loaded_at
from current_snapshot
