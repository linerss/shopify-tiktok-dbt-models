/*
  Shopify Refunds (Shopify orders only, excludes TikTok)

  Transforms refund data from Shopify and adds:
  - PST timezone conversions
  - Brand full names
  - Email addresses from order line items
  - Filters OUT TikTok orders (TikTok refunds are handled as cancellations in order line items)

  NOTE: Shipping refunds can come from two sources in Daton staging:
  - record_type = 'refund_shipping_line' (from Daton's refund_shipping_lines column)
  - record_type = 'adjustment' with a_kind = 'shipping_refund' (from order_adjustments)

  These two sources represent the SAME data (99% overlap with matching amounts).
  We use ONLY shipping_refund adjustments to:
  1. Avoid double-counting
  2. Capture 4 additional refunds that only exist in adjustments
  3. Maintain compatibility with Airbyte (which only has adjustments)
*/

{{ config(
    materialized='view'
) }}

with brand_map as (
    {{ store_brand_mapping_sql() }}
),

-- Union Daton (all brands except store_a) with Airbyte (store_a only)
-- Cast to preserve production types (ORDER_ID/REFUND_RECORD_ID as NUMBER)
staging as (
    select
        source_platform, brand, order_id, refund_record_id,
        refund_line_item_id, record_type, created_at, note,
        a_amount, a_kind, a_currency_code,
        rli_product_sku, rli_product_title, rli_product_quantity, rli_refund_subtotal, rli_currency_code,
        rsl_shipping_title, rsl_shipping_discounted_price, rsl_currency_code,
        daton_batch_date, ds
    from {{ ref('stg_shopify_refund_line_items') }}
    where upper(brand) NOT IN ({{ migrated_shopify_store_codes() }})

    union all

    select
        source_platform, brand,
        order_id::number as order_id,
        refund_record_id::number as refund_record_id,
        refund_line_item_id, record_type, created_at, note,
        a_amount, a_kind, a_currency_code,
        rli_product_sku, rli_product_title, rli_product_quantity, rli_refund_subtotal, rli_currency_code,
        rsl_shipping_title, rsl_shipping_discounted_price::float as rsl_shipping_discounted_price, rsl_currency_code,
        daton_batch_date::timestamp_ntz as daton_batch_date, ds::timestamp_ntz as ds
    from {{ ref('stg_airbyte_shopify_refund_line_items') }}
),

order_emails as (
    select distinct order_id, email
    from {{ ref('stg_shopify_order_line_items') }}
    where source_name != 'TikTok'

    union

    select distinct order_id, email
    from {{ ref('stg_airbyte_shopify_order_line_items') }}
)

select
    -- Dates (created_at is timestamp_tz from Shopify)
    convert_timezone('UTC','America/Los_Angeles', src.created_at::timestamp) as refund_date,
    date(convert_timezone('UTC','America/Los_Angeles', src.created_at::timestamp)) as refund_date_pt,
    convert_timezone('UTC','America/Los_Angeles', src.created_at::timestamp) as refund_ts_pt,
    src.created_at::timestamp as refund_ts_utc,
    date(src.created_at::timestamp) as refund_date_utc,

    -- Keys
    -- For shipping_refund adjustments, use 'refund_shipping_line' in the key for consistency
    CONCAT(src.source_platform, '_', src.brand, '_', CAST(CAST(src.refund_line_item_id as int) as string), '_',
        case when src.a_kind = 'shipping_refund' then 'refund_shipping_line' else src.record_type end) as refund_key,
    CONCAT(src.source_platform, '_', src.brand, '_',
        CAST(case when src.rsl_shipping_title is not null or src.a_kind = 'shipping_refund' then 'shipping' else src.rli_product_sku end as string)) as product_key,
    -- Normalize shipping_refund adjustments to 'refund_shipping_line' for downstream compatibility
    case when src.a_kind = 'shipping_refund' then 'refund_shipping_line' else src.record_type end as record_type,
    src.source_platform,
    UPPER(src.brand) as brand,
    coalesce(bm.brand_full, upper(src.brand)) as brand_full,
    src.refund_record_id,
    src.refund_line_item_id,
    src.order_id,
    case
        when src.rsl_shipping_title is not null or src.a_kind = 'shipping_refund' then 'shipping'
        else src.rli_product_sku
    end as rli_product_sku,
    src.rli_product_title,
    -- For shipping_refund adjustments, use a_kind as the "title" indicator
    coalesce(src.rsl_shipping_title, case when src.a_kind = 'shipping_refund' then 'Shipping Refund' end) as rsl_shipping_title,

    -- Amounts
    upper(trim(COALESCE(src.a_currency_code, src.rli_currency_code, src.rsl_currency_code))) as currency,
    COALESCE(src.a_amount*-1, -1 * src.rli_refund_subtotal) as refund_amount,
    src.rli_product_quantity,
    -- For shipping_refund adjustments, use a_amount (already negative in adjustments, so negate to make positive then negate for output)
    case
        when src.a_kind = 'shipping_refund' then src.a_amount  -- a_amount is already the refund amount (typically negative)
        else -1 * src.rsl_shipping_discounted_price
    end as shipping_refund_amount,

    -- Metadata
    src.note,
    src.daton_batch_date,
    src.ds,
    -- Customer PII columns (email, name, phone, and street address) removed for open-source release.
    current_timestamp() as dbt_last_updated,
    '{{ env_var("DBT_CLOUD_RUN_ID", "manual") }}' as dbt_run_id
from staging src
left join brand_map bm
    on bm.brand = upper(src.brand)
inner join order_emails
    on src.order_id = order_emails.order_id
where src.refund_line_item_id is not null
  -- Include refund_line_items (products) and shipping_refund adjustments only
  -- Exclude refund_shipping_line (duplicates shipping_refund adjustments with 99% overlap)
  -- Exclude other adjustment types (e.g., refund_discrepancy) which are summary records
  and (src.record_type = 'refund_line_item' or src.a_kind = 'shipping_refund')