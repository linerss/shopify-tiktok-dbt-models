{{ config(
    materialized='incremental',
    unique_key=['brand', 'statement_id', 'transaction_id'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

-- TikTok statement transactions - joining flattened tables

{% if is_incremental() %}
with max_timestamps as (
    select brand, max(daton_batch_runtime) as max_batch_runtime
    from {{ this }}
    group by brand
)
{% else %}
with max_timestamps as (select null as brand, null as max_batch_runtime where false)
{% endif %}

, statements as (
    select
        'STORE_D' as brand,
        create_time,
        currency,
        id as statement_id,
        status,
        payable_amount,
        total_reserve_amount,
        total_settlement_amount,
        total_settlement_breakdown,
        daton_batch_id,
        daton_batch_runtime
    from {{ source('tiktok', 'store_d_tiktok_statement_transactions') }}
    {% if is_incremental() %}
    where daton_batch_runtime >= (select max_batch_runtime from max_timestamps where brand = 'STORE_D')
    {% endif %}

    {# union all

    select
        'STORE_C' as brand,
        create_time,
        currency,
        id as statement_id,
        status,
        payable_amount,
        total_reserve_amount,
        total_settlement_amount,
        total_settlement_breakdown,
        daton_batch_id,
        daton_batch_runtime
    from {{ source('tiktok', 'store_c_tiktok_statement_transactions') }} #}

    union all

    select
        'STORE_A' as brand,
        create_time,
        currency,
        id as statement_id,
        status,
        payable_amount,
        total_reserve_amount,
        total_settlement_amount,
        total_settlement_breakdown,
        daton_batch_id,
        daton_batch_runtime
    from {{ source('tiktok', 'store_a_tiktok_statement_transactions') }}
    {% if is_incremental() %}
    where daton_batch_runtime >= (select max_batch_runtime from max_timestamps where brand = 'STORE_A')
    {% endif %}
)

, transactions as (
    select
        'STORE_D' as brand,
        id as transaction_id,
        type as transaction_type,
        order_id,
        order_create_time,
        adjustment_amount,
        adjustment_id,
        adjustment_order_id,
        associated_order_id,
        estimated_release_time,
        fee_tax_amount,
        reserve_amount,
        reserve_id,
        reserve_status,
        revenue_amount,
        settlement_amount,
        shipping_cost_amount,
        daton_batch_id,
        daton_batch_runtime,
        daton_parent_batch_id
    from {{ source('tiktok', 'store_d_tiktok_statement_transactions_transactions') }}
    {% if is_incremental() %}
    where daton_batch_runtime >= (select max_batch_runtime from max_timestamps where brand = 'STORE_D')
    {% endif %}

    {# union all

    select
        'STORE_C' as brand,
        id as transaction_id,
        type as transaction_type,
        order_id,
        order_create_time,
        adjustment_amount,
        adjustment_id,
        adjustment_order_id,
        associated_order_id,
        estimated_release_time,
        fee_tax_amount,
        reserve_amount,
        reserve_id,
        reserve_status,
        revenue_amount,
        settlement_amount,
        shipping_cost_amount,
        daton_batch_id,
        daton_batch_runtime,
        daton_parent_batch_id
    from {{ source('tiktok', 'store_c_tiktok_statement_transactions_transactions') }} #}

    union all

    select
        'STORE_A' as brand,
        id as transaction_id,
        type as transaction_type,
        order_id,
        order_create_time,
        adjustment_amount,
        adjustment_id,
        adjustment_order_id,
        associated_order_id,
        estimated_release_time,
        fee_tax_amount,
        reserve_amount,
        reserve_id,
        reserve_status,
        revenue_amount,
        settlement_amount,
        shipping_cost_amount,
        daton_batch_id,
        daton_batch_runtime,
        daton_parent_batch_id
    from {{ source('tiktok', 'store_a_tiktok_statement_transactions_transactions') }}
    {% if is_incremental() %}
    where daton_batch_runtime >= (select max_batch_runtime from max_timestamps where brand = 'STORE_A')
    {% endif %}
)

, fee_breakdown as (
    select
        'STORE_D' as brand,
        parse_json(fee)[0] as fee_json,
        parse_json(tax)[0] as tax_json,
        daton_batch_id,
        daton_batch_runtime,
        daton_parent_batch_id
    from {{ source('tiktok', 'store_d_tiktok_statement_transactions_transactions_fee_tax_breakdown') }}
    {% if is_incremental() %}
    where daton_batch_runtime >= (select max_batch_runtime from max_timestamps where brand = 'STORE_D')
    {% endif %}

    {# union all

    select
        'STORE_C' as brand,
        parse_json(fee)[0] as fee_json,
        parse_json(tax)[0] as tax_json,
        daton_batch_id,
        daton_batch_runtime,
        daton_parent_batch_id
    from {{ source('tiktok', 'store_c_tiktok_statement_transactions_transactions_fee_tax_breakdown') }} #}

    union all

    select
        'STORE_A' as brand,
        parse_json(fee)[0] as fee_json,
        parse_json(tax)[0] as tax_json,
        daton_batch_id,
        daton_batch_runtime,
        daton_parent_batch_id
    from {{ source('tiktok', 'store_a_tiktok_statement_transactions_transactions_fee_tax_breakdown') }}
    {% if is_incremental() %}
    where daton_batch_runtime >= (select max_batch_runtime from max_timestamps where brand = 'STORE_A')
    {% endif %}
)

, joined as (
    select
        s.brand,
        s.create_time,
        to_timestamp(s.create_time) as create_timestamp,
        s.currency,
        s.statement_id,
        s.status as statement_status,
        s.payable_amount,
        s.total_reserve_amount,
        s.total_settlement_amount,
        t.transaction_id,
        t.transaction_type,
        t.order_id,
        to_timestamp(t.order_create_time) as order_create_time,
        t.adjustment_amount,
        t.adjustment_id,
        t.adjustment_order_id,
        t.associated_order_id,
        t.estimated_release_time,
        t.fee_tax_amount,
        t.reserve_amount,
        t.reserve_id,
        t.reserve_status,
        t.revenue_amount,
        t.settlement_amount,
        t.shipping_cost_amount,
        fb.fee_json,
        fb.tax_json,
        s.daton_batch_id,
        s.daton_batch_runtime
    from statements s
    inner join transactions t
        on s.brand = t.brand
        and s.daton_batch_id = t.daton_parent_batch_id
        and s.daton_batch_runtime = t.daton_batch_runtime
    left join fee_breakdown fb
        on t.brand = fb.brand
        and t.daton_batch_id = fb.daton_parent_batch_id
        and t.daton_batch_runtime = fb.daton_batch_runtime
)

select
    brand,
    create_time,
    create_timestamp,
    currency,
    statement_id,
    statement_status,
    payable_amount,
    total_reserve_amount,
    total_settlement_amount,
    transaction_id,
    transaction_type,
    order_id,
    order_create_time,
    adjustment_amount,
    adjustment_id,
    adjustment_order_id,
    associated_order_id,
    estimated_release_time,
    fee_tax_amount,
    reserve_amount,
    reserve_id,
    reserve_status,
    revenue_amount,
    settlement_amount,
    shipping_cost_amount,
    fee_json,
    tax_json,
    daton_batch_id,
    daton_batch_runtime
from joined