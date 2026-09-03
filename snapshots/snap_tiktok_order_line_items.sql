{% snapshot snap_tiktok_order_line_items %}

{{
    config(
        target_schema='snapshots',
        unique_key='tiktok_order_key',
        strategy='timestamp',
        updated_at='update_time',
        invalidate_hard_deletes=false
    )
}}

-- One row per key first. See snap_shopify_order_line_items for why.

select
    concat_ws('|', brand, order_id) as tiktok_order_key,
    *
from {{ ref('stg_tiktok_order_line_items') }}
qualify
    row_number() over (
        partition by brand, order_id
        order by update_time desc, daton_batch_runtime_ts desc
    ) = 1

{% endsnapshot %}
