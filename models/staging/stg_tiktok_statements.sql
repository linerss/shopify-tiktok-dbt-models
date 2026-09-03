{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['store_id', 'statement_id'],
    on_schema_change='sync_all_columns'
) }}

{% set stores = var('tiktok_shop_stores', [
    {'store_id': 'store_a', 'statements_table': 'store_a_statements'},
    {'store_id': 'store_b', 'statements_table': 'store_b_statements'}
]) %}

with unioned as (
    {% for store in stores %}
    select
        'tiktok_shop' as source_platform,
        '{{ store.store_id }}' as store_id,
        {{ normalize_numeric_id('src.value:"statement_id"') }} as statement_id,
        src.value:"statement_status"::string as statement_status,
        src.value:"settlement_amount"::number(18, 4) as settlement_amount,
        src.value:"currency"::string as currency,
        src.value:"statement_start_time"::timestamp_tz as statement_start_at,
        src.value:"statement_end_time"::timestamp_tz as statement_end_at,
        src.value:"payment_time"::timestamp_tz as paid_at,
        to_timestamp_tz(src.source_batch_runtime_ms / 1000) as source_loaded_at
    from {{ source('tiktok_shop', store.statements_table) }} as src
    {% if is_incremental() %}
    where to_timestamp_tz(src.source_batch_runtime_ms / 1000) > (
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
    statement_status,
    settlement_amount,
    currency,
    statement_start_at,
    statement_end_at,
    paid_at,
    source_loaded_at
from unioned
qualify row_number() over (
    partition by store_id, statement_id
    order by source_loaded_at desc
) = 1
