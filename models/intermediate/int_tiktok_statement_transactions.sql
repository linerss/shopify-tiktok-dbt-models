{{ config(materialized='table') }}

{#
  Keep one row per platform transaction. Fee details stay in a structured object
  so transaction_amount appears exactly once and cannot multiply at fee grain.
#}
select
    concat_ws('|', source_platform, store_id, statement_id, transaction_id) as statement_transaction_key,
    source_platform,
    store_id,
    {{ normalize_numeric_id('statement_id') }} as statement_id,
    {{ normalize_numeric_id('transaction_id') }} as transaction_id,
    {{ normalize_numeric_id('order_id') }} as order_id,
    transaction_type,
    transaction_amount,
    currency,
    fee_breakdown,
    transaction_at,
    source_loaded_at
from {{ ref('stg_tiktok_statement_transactions') }}
qualify row_number() over (
    partition by source_platform, store_id, statement_id, transaction_id
    order by source_loaded_at desc
) = 1
