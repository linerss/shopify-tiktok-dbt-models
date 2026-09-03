select
    fee_event_key,
    count(*) as duplicate_count
from {{ ref('fct_tiktok_statement_fees') }}
group by fee_event_key
having count(*) > 1
