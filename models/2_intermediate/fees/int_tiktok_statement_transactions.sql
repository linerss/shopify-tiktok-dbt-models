{{ config(materialized='table') }}

-- TikTok statement transaction fees and commissions (pivot happens here)

-- Get order SKUs for joining
with order_skus as (
    select distinct
        upper(brand) as brand,
        order_id,
        seller_sku
    from {{ ref('int_tiktok_order_line_items') }}
    where order_id is not null
)

-- First get the fee breakdown
, fee_pivot as (
    select
        stg.brand,
        create_timestamp as posted_date,
        currency,
        statement_id,
        transaction_id,
        transaction_type,
        stg.order_id,
        order_create_time,
        sku.seller_sku,
        f.key as fee_type,
        -- Categorize fees
        case
            when f.key in ('referral_fee_amount') then 'REFERRAL_FEE'
            when f.key in ('affiliate_ads_commission_amount', 'tap_shop_ads_commission') then 'ADVERTISING_FEE'
            when f.key in ('flash_sales_service_fee_amount') then 'PROMOTION_FEE'
            when f.key in ('refund_administration_fee_amount') then 'REFUND_FEE'
            when f.key in ('affiliate_partner_commission_amount') then 'AFFILIATE_COMMISSION'
            when f.key in ('cofunded_promotion_service_fee_amount') then 'COFUNDED_PROMOTION_FEE'
            else 'OTHER'
        end as fee_category,
        f.value::number(38,2) as amount,
        daton_batch_id,
        daton_batch_runtime,
        to_timestamp(daton_batch_runtime/1000) as daton_batch_date
    from {{ ref('stg_tiktok_statement_transactions') }} as stg
    left join order_skus sku
        on sku.brand = stg.brand
        and sku.order_id = stg.order_id
    , lateral flatten(input => fee_json) f
    where f.value::number(38,2) is not null
      and f.value::number(38,2) <> 0
      and f.key != 'affiliate_commission_amount_before_pit'
)

-- Add adjustments
, adjustments as (
    select
        stg.brand,
        create_timestamp as posted_date,
        currency,
        statement_id,
        transaction_id,
        transaction_type,
        stg.order_id,
        order_create_time,
        sku.seller_sku,
        'adjustment_amount' as fee_type,
        'ADJUSTMENT' as fee_category,
        adjustment_amount::number(38,2) as amount,
        daton_batch_id,
        daton_batch_runtime,
        to_timestamp(daton_batch_runtime/1000) as daton_batch_date
    from {{ ref('stg_tiktok_statement_transactions') }} as stg
    left join order_skus sku
        on sku.brand = stg.brand
        and sku.order_id = stg.order_id
    where adjustment_amount::number(38,2) is not null
      and adjustment_amount::number(38,2) <> 0
)

-- Combine fees and adjustments
select * from fee_pivot
union all
select * from adjustments
order by posted_date, brand, transaction_id, fee_type