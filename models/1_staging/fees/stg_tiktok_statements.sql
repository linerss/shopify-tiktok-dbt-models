{{ config(
    materialized='incremental',
    unique_key=['brand', 'statement_id'],
    incremental_strategy='merge',
    on_schema_change='sync_all_columns'
) }}

{% set brands = tiktok_statement_sources() %}

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

    {% for b in brands %}

    select
        'TikTok' as source_platform,
        '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as dbt_run_id,
        '{{ b.brand }}' as brand,
        id as statement_id,
        payment_id,
        payment_status,
        
        adjustment_amount::float as adjustment_amount,
        fee_amount::float as fee_amount,
        net_sales_amount::float as net_sales_amount,
        revenue_amount::float as revenue_amount,
        settlement_amount::float as settlement_amount,
        shipping_cost_amount::float as shipping_cost_amount,
        currency,

        to_timestamp_ntz(statement_time) as statement_time,
        date_trunc('day', to_timestamp_ntz(statement_time)) as ds,

        daton_batch_id,
        daton_batch_runtime,
        to_timestamp_ntz(daton_batch_runtime / 1000) as daton_batch_date,

        current_timestamp() as dbt_last_updated,
        row_number() over(partition by id order by daton_batch_runtime) as rn

    from {{ source('tiktok', b.source) }}

    {% if is_incremental() %}
      where daton_batch_runtime >= (
          select max_batch_runtime from max_timestamps where brand = '{{ b.brand }}'
      )
    {% endif %}

    qualify rn = 1

    {% if not loop.last %}
    union all
    {% endif %}

    {% endfor %}

)

select *
from statements