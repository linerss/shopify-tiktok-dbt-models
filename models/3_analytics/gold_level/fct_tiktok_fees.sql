{{ config(materialized="table") }}

with
    -- TikTok statement transaction fees and adjustments (already lateral flattened in int table)
    tiktok_fees as (
        select
            convert_timezone('America/Los_Angeles', posted_date)::date as fee_date,
            convert_timezone('America/Los_Angeles', posted_date)::date as posted_date_pt,
            upper(brand) as brand,
            'TikTok' as source_platform,
            upper(trim(currency)) as currency,
            cast(null as string) as marketplace_name,
            transaction_id,
            order_id,
            seller_sku as sku,
            fee_category,
            fee_type,
            cast(null as string) as fee_source,
            cast(null as int) as units_count,
            sum(amount) as charge
        from {{ ref('int_tiktok_statement_transactions') }}
        where amount is not null
            and amount <> 0
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
    ),

    -- TikTok shipping costs from statements
    tiktok_shipping as (
        select
            convert_timezone('America/Los_Angeles', posted_date)::date as fee_date,
            convert_timezone('America/Los_Angeles', posted_date)::date as posted_date_pt,
            upper(brand) as brand,
            'TikTok' as source_platform,
            upper(trim(currency)) as currency,
            cast(null as string) as marketplace_name,
            cast(null as string) as transaction_id,
            cast(null as string) as order_id,
            cast(null as string) as sku,
            'SHIPPING_COST' as fee_category,
            'shipping_cost_amount' as fee_type,
            cast(null as string) as fee_source,
            cast(null as int) as units_count,
            sum(shipping_cost_amount) as charge
        from {{ ref('int_tiktok_statements') }}
        where shipping_cost_amount is not null
            and shipping_cost_amount <> 0
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
    ),

    -- Combine all fees
    combined as (
        select * from tiktok_fees
        union all
        select * from tiktok_shipping
    )

-- Final output
select
    fee_date,
    posted_date_pt,
    brand,
    source_platform,
    currency,
    marketplace_name,
    transaction_id,
    order_id,
    sku,
    fee_category,
    fee_type,
    fee_source,
    units_count,
    charge,
    -- USD conversion (TikTok is USD only for now, but following same pattern as Amazon)
    case
        when currency = 'USD' then charge
        else charge
    end as charge_usd
from combined
order by fee_date, fee_category, fee_type
