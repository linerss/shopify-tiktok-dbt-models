/*
  Daton → Airbyte migration: store_a uses Airbyte staging, all other brands use Daton staging.
  The staging CTE unions both sources with only the columns this model needs.
*/

{{ config(
    materialized='table'
) }}

with
    brand_map as (
        {{ store_brand_mapping_sql() }}
    ),

    -- Union Daton (all brands except store_a) with Airbyte (store_a only)
    -- Explicit column list avoids type mismatches on unused columns between Daton (TEXT/JSON strings) and Airbyte (native types)
    staging as (
        select
            source_platform, source_name, brand, order_id, order_name_number,
            line_item_id, product_sku, product_title, item_name,
            quantity, product_price, line_item_discount_per_unit,
            processed_at, event_date, updated_at, closed_at, closed_at_pst,
            currency, financial_status, fulfillment_status,
            -- Customer PII columns (email, name, phone, and street address) removed for open-source release.
            tags, row_type, ds,
            client_order_id, order_type, card_last4,
            utm_source, utm_medium, utm_campaign, utm_term, utm_content,
            custom1, custom2, note_attributes_map
        from {{ ref("stg_shopify_order_line_items") }}
        where upper(brand) NOT IN ({{ migrated_shopify_store_codes() }})

        union all

        select
            source_platform, source_name, brand, order_id, order_name_number,
            line_item_id, product_sku, product_title, item_name,
            quantity, product_price, line_item_discount_per_unit,
            processed_at::timestamp_tz as processed_at,
            event_date::timestamp_tz as event_date,
            updated_at, closed_at, closed_at_pst,
            currency, financial_status, fulfillment_status,
            tags, row_type,
            ds::timestamp_ntz as ds,
            client_order_id, order_type, card_last4,
            utm_source, utm_medium, utm_campaign, utm_term, utm_content,
            custom1, custom2, note_attributes_map
        from {{ ref("stg_airbyte_shopify_order_line_items") }}
    ),

    source as (
        select
            -- Identifiers
            stg.source_platform,
            stg.source_name,
            upper(stg.brand) as brand,
            stg.order_id,
            stg.order_name_number,
            try_to_number(stg.line_item_id)::int::string as line_item_id,  -- Normalize decimal formats from Daton (e.g., 123.000000000 -> 123)
            case
                when stg.product_sku = ''
                then stg.product_title
                else coalesce(stg.product_sku, stg.product_title)
            end as product_sku,
            stg.item_name,

            -- Keys
            concat_ws('_', stg.source_platform, stg.brand, stg.order_id, stg.line_item_id, stg.row_type) as order_line_key,
            concat(
                stg.source_platform,
                '_',
                upper(stg.brand),
                '_',
                case
                    when stg.product_sku = ''
                    then stg.product_title
                    else coalesce(stg.product_sku, stg.product_title)
                end
            ) as product_key,

            -- Dates
            stg.processed_at as purchase_date,
            convert_timezone('America/Los_Angeles', stg.processed_at) as purchase_date_pst,
            stg.event_date,
            convert_timezone('America/Los_Angeles', stg.event_date) as event_date_pst,
            convert_timezone('UTC', stg.updated_at)::timestamp_ntz(9) as last_updated_date,
            convert_timezone('America/Los_Angeles', stg.updated_at)::timestamp_ntz(9) as last_updated_date_pst,
            stg.closed_at,
            stg.closed_at_pst,

            -- Pricing
            upper(trim(stg.currency)) as currency,
            stg.product_price as list_price,
            stg.product_price as list_price_per_unit,
            round(stg.line_item_discount_per_unit * stg.quantity, 2) as seller_discount,
            stg.line_item_discount_per_unit as seller_discount_per_unit,
            null as platform_discount,

            -- Quantities
            stg.quantity as units,

            -- Sales amount
            round((stg.product_price - stg.line_item_discount_per_unit) * stg.quantity, 2) as sales_amount,
            cast((stg.product_price - stg.line_item_discount_per_unit) as number(38, 2)) as sales_amount_per_unit,

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
        where stg.source_name != 'tiktok'  -- Filter out TikTok orders (handled separately in int_tiktok_order_line_items)
        qualify
            row_number() over (
                partition by
                    stg.source_platform,
                    stg.brand,
                    stg.order_id,
                    try_to_number(stg.line_item_id)::int::string,  -- Use normalized line_item_id for deduplication
                    stg.row_type
                order by convert_timezone('UTC', stg.updated_at)::timestamp_ntz(9) desc
            ) = 1
    )

select
    s.*,
    coalesce(bm.brand_full, s.brand) as brand_full
from source s
left join brand_map bm on bm.brand = s.brand
