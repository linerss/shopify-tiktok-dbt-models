select
    statement_transaction_key,
    count(*) as duplicate_count
from {{ ref('int_tiktok_statement_transactions') }}
group by statement_transaction_key
having count(*) > 1
