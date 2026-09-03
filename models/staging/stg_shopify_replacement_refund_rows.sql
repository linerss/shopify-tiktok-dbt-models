{{ config(materialized='table') }}

{#
  This connector exposes product refunds and order adjustments. Shipping
  refunds are therefore represented by adjustments where kind=shipping_refund.
#}
{% set stores = var('shopify_replacement_stores', [
    {'store_id': 'store_a', 'source': 'shopify_replacement_store_a'},
    {'store_id': 'store_b', 'source': 'shopify_replacement_store_b'}
]) %}

with source_refunds as (
    {% for store in stores %}
    select
        '{{ store.store_id }}' as store_id,
        src.id,
        src.order_id,
        src.refund_line_items,
        src.order_adjustments,
        src.currency,
        src.processed_at,
        src.updated_at,
        src._connector_emitted_at
    from {{ source(store.source, 'order_refunds') }} as src
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

product_refunds as (
    select
        'shopify' as source_platform,
        'replacement' as connector,
        src.store_id,
        {{ normalize_numeric_id('src.id') }} as refund_id,
        {{ normalize_numeric_id('src.order_id') }} as order_id,
        'refund_line_item' as record_type,
        {{ normalize_numeric_id('item.value:"id"') }} as record_id,
        {{ normalize_numeric_id('item.value:"line_item_id"') }} as line_item_id,
        null::string as adjustment_kind,
        item.value:"quantity"::number as quantity,
        item.value:"subtotal"::number(18, 4) as amount,
        src.currency::string as currency,
        src.processed_at::timestamp_tz as processed_at,
        src.updated_at::timestamp_tz as updated_at,
        src._connector_emitted_at::timestamp_tz as source_loaded_at
    from source_refunds as src,
    lateral flatten(input => src.refund_line_items) as item
),

adjustments as (
    select
        'shopify' as source_platform,
        'replacement' as connector,
        src.store_id,
        {{ normalize_numeric_id('src.id') }} as refund_id,
        {{ normalize_numeric_id('src.order_id') }} as order_id,
        'adjustment' as record_type,
        {{ normalize_numeric_id('adjustment.value:"id"') }} as record_id,
        null::string as line_item_id,
        adjustment.value:"kind"::string as adjustment_kind,
        1::number as quantity,
        adjustment.value:"amount"::number(18, 4) as amount,
        src.currency::string as currency,
        src.processed_at::timestamp_tz as processed_at,
        src.updated_at::timestamp_tz as updated_at,
        src._connector_emitted_at::timestamp_tz as source_loaded_at
    from source_refunds as src,
    lateral flatten(input => src.order_adjustments) as adjustment
)

select source_platform, connector, store_id, refund_id, order_id, record_type, record_id, line_item_id, adjustment_kind, quantity, amount, currency, processed_at, updated_at, source_loaded_at
from product_refunds
union all
select source_platform, connector, store_id, refund_id, order_id, record_type, record_id, line_item_id, adjustment_kind, quantity, amount, currency, processed_at, updated_at, source_loaded_at
from adjustments
