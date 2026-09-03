{{
  config(
    materialized = "incremental",
    incremental_strategy = "append",
    on_schema_change = 'sync_all_columns'
  )
}}

-- TikTok Returns staging model
-- Used to determine actual refunds vs replacements/exchanges
-- return_type values:
--   REFUND: money back to customer (should create cancellation row)
--   RETURN_AND_REFUND: money back to customer (should create cancellation row)
--   REPLACEMENT: no money back, customer gets new item (keep original sale)
--   EXCHANGE: no money back, customer gets different item (keep original sale)

{% set brands = tiktok_return_sources() %}

with source_data as (
    {% for b in brands %}
    select
        'TikTok' as source_platform,
        '{{ b.brand }}' as brand,
        order_id,
        return_id,
        return_type,
        return_status,
        return_reason,
        return_reason_text,

        -- Parse refund amounts from JSON array
        -- refund_amount is a JSON array like [{"currency":"USD","refund_total":"33.67",...}]
        try_parse_json(refund_amount)[0]:"refund_total"::float as refund_total,
        try_parse_json(refund_amount)[0]:"refund_subtotal"::float as refund_subtotal,
        try_parse_json(refund_amount)[0]:"refund_shipping_fee"::float as refund_shipping_fee,
        try_parse_json(refund_amount)[0]:"refund_tax"::float as refund_tax,
        try_parse_json(refund_amount)[0]:"currency"::string as refund_currency,

        to_timestamp_ntz(create_time) as create_time,
        to_timestamp_ntz(update_time) as update_time,

        daton_batch_id,
        to_timestamp_ltz(daton_batch_runtime / 1000) as daton_batch_runtime_ts,
        date_trunc('day', to_timestamp_ltz(daton_batch_runtime / 1000)) as daton_batch_date

    from {{ source('tiktok', b.source) }}

    {% if is_incremental() %}
    where to_timestamp_ltz(daton_batch_runtime / 1000) > (
        select coalesce(max(daton_batch_runtime_ts), '1900-01-01'::timestamp_ltz)
        from {{ this }}
        where brand = '{{ b.brand }}'
    )
    {% endif %}

    {% if not loop.last %}
    union all
    {% endif %}
    {% endfor %}
)

select
    *,
    -- Flag for whether this return results in money back to customer
    case
        when return_type in ('REFUND', 'RETURN_AND_REFUND') then true
        else false
    end as is_actual_refund,

    current_timestamp() as dbt_last_updated,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as dbt_run_id
from source_data
