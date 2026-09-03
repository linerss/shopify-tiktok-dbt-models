{{ config(materialized='table') }}

{#
  Amounts remain in native currency. Transaction totals deliberately stay in
  the transaction model because repeating them once per fee multiplies totals.
#}
with flattened_fees as (
    select
        transaction.source_platform,
        transaction.store_id,
        transaction.statement_id,
        transaction.transaction_id,
        transaction.order_id,
        transaction.transaction_type,
        fee.key::string as fee_type,
        fee.value::number(18, 4) as fee_amount,
        transaction.currency as native_currency,
        transaction.transaction_at,
        transaction.source_loaded_at
    from {{ ref('int_tiktok_statement_transactions') }} as transaction,
    lateral flatten(input => transaction.fee_breakdown) as fee
)

select
    concat_ws('|', source_platform, store_id, statement_id, transaction_id, fee_type) as fee_event_key,
    store_id,
    statement_id,
    transaction_id,
    order_id,
    transaction_type,
    fee_type,
    case
        when lower(fee_type) like '%commission%' then 'commission'
        when lower(fee_type) like '%shipping%' then 'shipping'
        when lower(fee_type) like '%tax%' then 'tax'
        when lower(fee_type) like '%affiliate%' then 'affiliate'
        else 'other'
    end as fee_category,
    fee_amount,
    native_currency,
    transaction_at,
    source_loaded_at
from flattened_fees
