/*
  Daton → Airbyte migration: store_a uses Airbyte staging, all other brands use Daton staging.
*/

{{ config(
    materialized='table'
) }}

with brand_map as (
    {{ store_brand_mapping_sql() }}
),

-- Union Daton (all brands except store_a) with Airbyte (store_a only)
staging as (
    select
        source_platform, source_name, brand, order_id, order_name_number,
        shipping_line_id, shipping_price, shipping_discount, shipping_title,
        processed_at, event_date, updated_at,
        currency, financial_status, fulfillment_status,
        -- Customer PII columns (email, name, phone, and street address) removed for open-source release.
        tags, row_type, ds,
        client_order_id, order_type, card_last4,
        utm_source, utm_medium, utm_campaign, utm_term, utm_content,
        custom1, custom2, note_attributes_map
    from {{ ref('stg_shopify_shipping_line_items') }}
    where upper(brand) NOT IN ({{ migrated_shopify_store_codes() }})

    union all

    select
        source_platform, source_name, brand, order_id, order_name_number,
        shipping_line_id, shipping_price, shipping_discount, shipping_title,
        processed_at::timestamp_tz as processed_at,
        event_date::timestamp_tz as event_date,
        updated_at,
        currency, financial_status, fulfillment_status,
        tags, row_type,
        ds::timestamp_ntz as ds,
        client_order_id, order_type, card_last4,
        utm_source, utm_medium, utm_campaign, utm_term, utm_content,
        custom1, custom2, note_attributes_map
    from {{ ref('stg_airbyte_shopify_shipping_line_items') }}
),

source as (
    select
        -- Identifiers
        stg.source_platform,
        stg.source_name,
        stg.brand,
        stg.order_id,
        stg.order_name_number,
        stg.shipping_line_id as line_item_id,
        'shipping' as product_sku,
        'Shipping' as item_name,

        -- Keys
        concat_ws('_', stg.source_platform, stg.brand, stg.order_id, stg.shipping_line_id, 'shipping', stg.row_type) as order_line_key,
        concat(stg.source_platform, '_', stg.brand, '_', 'shipping') as product_key,

        -- Dates
        stg.processed_at::timestamp_tz as purchase_date,
        convert_timezone('America/Los_Angeles', stg.processed_at) as purchase_date_pst,
        stg.event_date,
        convert_timezone('America/Los_Angeles', stg.event_date) as event_date_pst,
        convert_timezone('UTC', stg.updated_at)::timestamp_tz as last_updated_date,
        convert_timezone('America/Los_Angeles', stg.updated_at) as last_updated_date_pst,

        -- Pricing
        upper(trim(stg.currency)) as currency,
        stg.shipping_price as shipping_price_total,
        coalesce(stg.shipping_discount, 0) as shipping_promotion_discount_total,

        -- Order info
        stg.row_type,
        stg.tags,
        concat_ws('-', stg.financial_status, stg.fulfillment_status) as status,

        -- Note attributes
        stg.client_order_id,
        stg.order_type,
        stg.card_last4,
        stg.utm_source,
        stg.utm_medium,
        stg.utm_campaign,
        stg.utm_term,
        stg.utm_content,
        stg.custom1,
        stg.custom2,
        stg.note_attributes_map,

        -- Metadata
        stg.ds::date as ds,
        current_timestamp() as dbt_last_updated,
        '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as dbt_run_id

    from staging as stg
    where stg.shipping_price <> 0
      and stg.source_name != 'tiktok'  -- Filter out TikTok orders (handled separately in int_tiktok_shipping_line_items)
),

deduplicated as (
    select
        s.*,
        row_number() over (
            partition by s.order_line_key, s.row_type
            order by s.last_updated_date desc
        ) as rn
    from source s
)

select
    d.source_platform,
    d.source_name,
    d.brand,
    d.order_id,
    d.order_name_number,
    d.line_item_id,
    d.product_sku,
    d.item_name,
    d.order_line_key,
    d.product_key,
    d.purchase_date,
    d.purchase_date_pst,
    d.event_date,
    d.event_date_pst,
    d.last_updated_date,
    d.last_updated_date_pst,
    d.currency,
    d.shipping_price_total,
    d.shipping_promotion_discount_total,
    d.row_type,
    d.tags,
    d.status,
    d.client_order_id,
    d.order_type,
    d.card_last4,
    d.utm_source,
    d.utm_medium,
    d.utm_campaign,
    d.utm_term,
    d.utm_content,
    d.custom1,
    d.custom2,
    d.note_attributes_map,
    d.ds,
    d.dbt_last_updated,
    d.dbt_run_id,
    coalesce(bm.brand_full, d.brand) as brand_full
from deduplicated d
left join brand_map bm
  on bm.brand = d.brand
where d.rn = 1
