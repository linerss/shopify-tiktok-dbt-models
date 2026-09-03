{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['store_id', 'statement_id', 'transaction_id'],
    on_schema_change='sync_all_columns'
) }}

{#
  Batch metadata reconstructs connector parent-child relationships. Stable
  platform IDs remain the business keys exposed downstream.
#}
{% set stores = var('tiktok_shop_stores', [
    {
      'store_id': 'store_a',
      'transactions_table': 'store_a_statement_transactions',
      'rows_table': 'store_a_statement_transaction_rows',
      'fee_rows_table': 'store_a_statement_fee_tax_rows'
    },
    {
      'store_id': 'store_b',
      'transactions_table': 'store_b_statement_transactions',
      'rows_table': 'store_b_statement_transaction_rows',
      'fee_rows_table': 'store_b_statement_fee_tax_rows'
    }
]) %}

with unioned as (
    {% for store in stores %}
    select
        'tiktok_shop' as source_platform,
        '{{ store.store_id }}' as store_id,
        {{ normalize_numeric_id('transaction_src.value:"statement_id"') }} as statement_id,
        {{ normalize_numeric_id('row_src.value:"transaction_id"') }} as transaction_id,
        {{ normalize_numeric_id('row_src.value:"order_id"') }} as order_id,
        row_src.value:"transaction_type"::string as transaction_type,
        row_src.value:"transaction_amount"::number(18, 4) as transaction_amount,
        row_src.value:"currency"::string as currency,
        fee.fee_breakdown,
        row_src.value:"transaction_time"::timestamp_tz as transaction_at,
        to_timestamp_tz(row_src.source_batch_runtime_ms / 1000) as source_loaded_at
    from {{ source('tiktok_shop', store.transactions_table) }} as transaction_src
    inner join {{ source('tiktok_shop', store.rows_table) }} as row_src
        on transaction_src.source_batch_id = row_src.source_batch_id
       and transaction_src.source_batch_index = row_src.parent_batch_index
    left join (
        select
            aggregated_fee.source_batch_id,
            aggregated_fee.parent_batch_index,
            object_agg(
                aggregated_fee.fee_type,
                aggregated_fee.fee_amount
            ) as fee_breakdown
        from (
            select
                fee_src.source_batch_id,
                fee_src.parent_batch_index,
                fee_src.value:"fee_type"::string as fee_type,
                sum(fee_src.value:"fee_amount"::number(18, 4)) as fee_amount
            from {{ source('tiktok_shop', store.fee_rows_table) }} as fee_src
            where fee_src.value:"fee_type"::string is not null
            group by
                fee_src.source_batch_id,
                fee_src.parent_batch_index,
                fee_src.value:"fee_type"::string
        ) as aggregated_fee
        group by aggregated_fee.source_batch_id, aggregated_fee.parent_batch_index
    ) as fee
        on row_src.source_batch_id = fee.source_batch_id
       and row_src.source_batch_index = fee.parent_batch_index
    {% if is_incremental() %}
    where to_timestamp_tz(row_src.source_batch_runtime_ms / 1000) > (
        select coalesce(max(source_loaded_at), '1900-01-01'::timestamp_tz)
        from {{ this }}
        where store_id = '{{ store.store_id }}'
    )
    {% endif %}
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
)

select
    source_platform,
    store_id,
    statement_id,
    transaction_id,
    order_id,
    transaction_type,
    transaction_amount,
    currency,
    fee_breakdown,
    transaction_at,
    source_loaded_at
from unioned
qualify row_number() over (
    partition by store_id, statement_id, transaction_id
    order by source_loaded_at desc
) = 1
