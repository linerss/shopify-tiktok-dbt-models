{{ config(materialized='table') }}

{% set migrated_stores = var('shopify_migrated_stores', ['store_a']) %}

with connector_union as (
    select
        source_platform,
        connector,
        store_id,
        refund_id,
        order_id,
        record_type,
        record_id,
        line_item_id,
        adjustment_kind,
        quantity,
        amount,
        currency,
        processed_at,
        updated_at,
        source_loaded_at
    from {{ ref('stg_shopify_legacy_refund_rows') }}
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
        refund_id,
        order_id,
        record_type,
        record_id,
        line_item_id,
        adjustment_kind,
        quantity,
        amount,
        currency,
        processed_at,
        updated_at,
        source_loaded_at
    from {{ ref('stg_shopify_replacement_refund_rows') }}
    {% if migrated_stores | length > 0 %}
    where store_id in (
        {% for store_id in migrated_stores %}'{{ store_id }}'{% if not loop.last %}, {% endif %}{% endfor %}
    )
    {% else %}
    where 1 = 0
    {% endif %}
),

canonical_refunds as (
    select
        source_platform,
        connector,
        store_id,
        {{ normalize_numeric_id('refund_id') }} as refund_id,
        {{ normalize_numeric_id('order_id') }} as order_id,
        case
            when record_type = 'refund_line_item' then 'product_refund'
            when record_type = 'adjustment' and lower(adjustment_kind) = 'shipping_refund' then 'shipping_refund'
        end as refund_type,
        {{ normalize_numeric_id('record_id') }} as refund_row_id,
        {{ normalize_numeric_id('line_item_id') }} as line_item_id,
        quantity,
        -abs(amount) as refund_amount,
        currency,
        processed_at,
        updated_at,
        source_loaded_at
    from connector_union
    where record_type = 'refund_line_item'
       or (record_type = 'adjustment' and lower(adjustment_kind) = 'shipping_refund')
)

select
    concat_ws('|', source_platform, store_id, refund_id, refund_type, refund_row_id) as refund_line_key,
    source_platform,
    connector,
    store_id,
    refund_id,
    order_id,
    refund_type,
    refund_row_id,
    line_item_id,
    quantity,
    refund_amount,
    currency,
    processed_at,
    updated_at,
    source_loaded_at
from canonical_refunds
qualify row_number() over (
    partition by source_platform, store_id, refund_id, refund_type, refund_row_id
    order by updated_at desc nulls last, source_loaded_at desc
) = 1
