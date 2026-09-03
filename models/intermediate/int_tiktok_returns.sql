{{ config(materialized='table') }}

{% set completed_return_statuses = var(
    'tiktok_completed_return_statuses',
    ['completed', 'refunded', 'closed']
) %}

with normalized as (
    select
        source_platform,
        store_id,
        {{ normalize_numeric_id('return_id') }} as return_id,
        {{ normalize_numeric_id('order_id') }} as order_id,
        {{ normalize_numeric_id('line_item_id') }} as line_item_id,
        lower(return_type) as return_type,
        lower(return_status) as return_status,
        refund_amount,
        currency,
        created_at,
        updated_at,
        source_loaded_at
    from {{ ref('stg_tiktok_returns') }}
)

select
    concat_ws('|', source_platform, store_id, return_id, coalesce(line_item_id, 'order')) as return_line_key,
    source_platform,
    store_id,
    return_id,
    order_id,
    line_item_id,
    return_type,
    return_status,
    {% if completed_return_statuses | length > 0 %}
    return_type in ('refund', 'return_and_refund')
        and return_status in (
            {% for status in completed_return_statuses %}'{{ status }}'{% if not loop.last %}, {% endif %}{% endfor %}
        )
    {% else %}
    false
    {% endif %} as reverses_revenue,
    return_type in ('replacement', 'exchange') as preserves_original_sale,
    refund_amount,
    currency,
    created_at,
    updated_at,
    source_loaded_at
from normalized
qualify row_number() over (
    partition by source_platform, store_id, return_id, coalesce(line_item_id, 'order')
    order by updated_at desc nulls last, source_loaded_at desc
) = 1
