{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['store_id', 'refund_id', 'record_type', 'record_id'],
    on_schema_change='sync_all_columns'
) }}

{% set stores = var('shopify_legacy_stores', [
    {'store_id': 'store_a', 'refund_table': 'store_a_refunds'},
    {'store_id': 'store_b', 'refund_table': 'store_b_refunds'}
]) %}

with source_refunds as (
    {% for store in stores %}
    select
        '{{ store.store_id }}' as store_id,
        src.value as refund,
        to_timestamp_tz(src.source_batch_runtime_ms / 1000) as source_loaded_at
    from {{ source('shopify_legacy', store.refund_table) }} as src
    {% if is_incremental() %}
    where to_timestamp_tz(src.source_batch_runtime_ms / 1000) > (
        select coalesce(max(source_loaded_at), '1900-01-01'::timestamp_tz)
        from {{ this }}
        where store_id = '{{ store.store_id }}'
    )
    {% endif %}
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

product_refunds as (
    select
        'shopify' as source_platform,
        'legacy' as connector,
        src.store_id,
        {{ normalize_numeric_id('src.refund:"id"') }} as refund_id,
        {{ normalize_numeric_id('src.refund:"order_id"') }} as order_id,
        'refund_line_item' as record_type,
        {{ normalize_numeric_id('item.value:"id"') }} as record_id,
        {{ normalize_numeric_id('item.value:"line_item_id"') }} as line_item_id,
        null::string as adjustment_kind,
        item.value:"quantity"::number as quantity,
        item.value:"subtotal"::number(18, 4) as amount,
        src.refund:"currency"::string as currency,
        src.refund:"processed_at"::timestamp_tz as processed_at,
        src.refund:"updated_at"::timestamp_tz as updated_at,
        src.source_loaded_at
    from source_refunds as src,
    lateral flatten(input => try_parse_json(src.refund:"refund_line_items"::string)) as item
),

shipping_refunds as (
    select
        'shopify' as source_platform,
        'legacy' as connector,
        src.store_id,
        {{ normalize_numeric_id('src.refund:"id"') }} as refund_id,
        {{ normalize_numeric_id('src.refund:"order_id"') }} as order_id,
        'refund_shipping_line' as record_type,
        {{ normalize_numeric_id('shipping.value:"id"') }} as record_id,
        null::string as line_item_id,
        null::string as adjustment_kind,
        1::number as quantity,
        shipping.value:"subtotal"::number(18, 4) as amount,
        src.refund:"currency"::string as currency,
        src.refund:"processed_at"::timestamp_tz as processed_at,
        src.refund:"updated_at"::timestamp_tz as updated_at,
        src.source_loaded_at
    from source_refunds as src,
    lateral flatten(input => try_parse_json(src.refund:"refund_shipping_lines"::string)) as shipping
),

adjustments as (
    select
        'shopify' as source_platform,
        'legacy' as connector,
        src.store_id,
        {{ normalize_numeric_id('src.refund:"id"') }} as refund_id,
        {{ normalize_numeric_id('src.refund:"order_id"') }} as order_id,
        'adjustment' as record_type,
        {{ normalize_numeric_id('adjustment.value:"id"') }} as record_id,
        null::string as line_item_id,
        adjustment.value:"kind"::string as adjustment_kind,
        1::number as quantity,
        adjustment.value:"amount"::number(18, 4) as amount,
        src.refund:"currency"::string as currency,
        src.refund:"processed_at"::timestamp_tz as processed_at,
        src.refund:"updated_at"::timestamp_tz as updated_at,
        src.source_loaded_at
    from source_refunds as src,
    lateral flatten(input => try_parse_json(src.refund:"order_adjustments"::string)) as adjustment
),

unioned as (
    select source_platform, connector, store_id, refund_id, order_id, record_type, record_id, line_item_id, adjustment_kind, quantity, amount, currency, processed_at, updated_at, source_loaded_at
    from product_refunds
    union all
    select source_platform, connector, store_id, refund_id, order_id, record_type, record_id, line_item_id, adjustment_kind, quantity, amount, currency, processed_at, updated_at, source_loaded_at
    from shipping_refunds
    union all
    select source_platform, connector, store_id, refund_id, order_id, record_type, record_id, line_item_id, adjustment_kind, quantity, amount, currency, processed_at, updated_at, source_loaded_at
    from adjustments
)

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
from unioned
qualify row_number() over (
    partition by store_id, refund_id, record_type, record_id
    order by source_loaded_at desc
) = 1
