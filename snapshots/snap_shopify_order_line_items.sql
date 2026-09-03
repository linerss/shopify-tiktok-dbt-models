{% snapshot snap_shopify_order_line_items %}

{{
    config(
        target_schema='snapshots',
        unique_key='order_line_key',
        strategy='timestamp',
        updated_at='updated_at_utc',
        invalidate_hard_deletes=false
    )
}}

-- Snapshot the CURRENT row per key, never the raw staging table.
-- Staging is append-only: it holds every version the connector ever sent, so
-- feeding it straight into a snapshot would compare a key against itself.
-- Reduce to one row per key first, then let dbt track the changes over time.

select
    concat_ws(
        '|',
        source_platform,
        brand,
        order_id,
        try_to_number(line_item_id)::int::string,
        row_type
    ) as order_line_key,
    convert_timezone('UTC', updated_at)::timestamp_ntz(9) as updated_at_utc,
    *
from {{ ref('stg_shopify_order_line_items') }}
qualify
    row_number() over (
        partition by
            source_platform,
            brand,
            order_id,
            try_to_number(line_item_id)::int::string,
            row_type
        order by convert_timezone('UTC', updated_at)::timestamp_ntz(9) desc
    ) = 1

{% endsnapshot %}
