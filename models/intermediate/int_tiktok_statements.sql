{{ config(materialized='table') }}

select
    concat_ws('|', source_platform, store_id, statement_id) as statement_key,
    source_platform,
    store_id,
    {{ normalize_numeric_id('statement_id') }} as statement_id,
    statement_status,
    settlement_amount,
    currency,
    statement_start_at,
    statement_end_at,
    paid_at,
    source_loaded_at
from {{ ref('stg_tiktok_statements') }}
qualify row_number() over (
    partition by source_platform, store_id, statement_id
    order by source_loaded_at desc
) = 1
