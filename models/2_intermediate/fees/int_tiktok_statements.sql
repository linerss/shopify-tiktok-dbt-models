{{ config(materialized='table') }}

with brand_map as (
    {{ store_brand_mapping_sql() }}
),

source as (
select
     concat(stg.source_platform, '_', stg.brand, '_', stg.statement_id) as event_key
     , stg.source_platform
     , stg.dbt_run_id
     , stg.brand
     , stg.statement_id
     , stg.payment_id
     , stg.payment_status
     , stg.adjustment_amount
     , stg.fee_amount
     , stg.net_sales_amount
     , stg.revenue_amount
     , stg.settlement_amount
     , stg.shipping_cost_amount
     , stg.currency
     , stg.statement_time as posted_date
     , stg.ds
     , stg.daton_batch_id
     , stg.daton_batch_date
     , stg.dbt_last_updated
from {{ ref('stg_tiktok_statements') }} as stg
)

select
    s.*,
    coalesce(bm.brand_full, s.brand) as brand_full
from source s
left join brand_map bm
  on bm.brand = s.brand