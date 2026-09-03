{% snapshot snap_shopify_shipping_line_items %}

{{
    config(
        target_schema='snapshots',
        unique_key='shipping_line_key',
        strategy='timestamp',
        updated_at='last_updated_date',
        invalidate_hard_deletes=false
    )
}}

-- One row per key first. See snap_shopify_order_line_items for why.

select
    concat_ws('|', order_line_key, row_type) as shipping_line_key,
    *
from {{ ref('stg_shopify_shipping_line_items') }}
qualify
    row_number() over (
        partition by order_line_key, row_type
        order by last_updated_date desc
    ) = 1

{% endsnapshot %}
