{{ config(
  materialized='incremental',
  incremental_strategy='append',
  on_schema_change='sync_all_columns'
) }}

-- Daton switched line_item_id to decimal strings on <CUTOVER_DATE>; cast to int to normalize.

{% set brands = tiktok_order_sources() %}

with
{% if is_incremental() %}
max_timestamps as (
    select brand, max(update_time) as max_date
    from {{ this }}
    group by brand
),
{% endif %}

flattened_line_items_raw as (

    {% for b in brands %}

    select
        -- Platform and brand identifiers
        'TikTok' as source_platform,
        upper('{{ b.brand }}') as brand,
        o.id::string as order_id,
        -- Customer PII columns (email, name, phone, and street address) removed for open-source release.
        o.tracking_number as order_tracking_number,

        -- Order details
        to_timestamp(o.delivery_time) as order_delivery_time,
        o.is_exchange_order,
        o.status as order_status,
        to_timestamp(o.paid_time) as paid_time,
        to_timestamp(o.create_time) as create_time,
        o.delivery_option_name,
        o.fulfillment_type,

        -- Additional order fields
        o.auto_combine_group_id,
        o.buyer_message,
        o.cancel_order_sla_time,
        o.cancel_reason as cancel_reason,
        to_timestamp(o.cancel_time) as cancel_time,
        o.cancellation_initiator,
        to_timestamp(o.collection_due_time) as collection_due_time,
        to_timestamp(o.collection_time) as collection_time,
        o.cpf,
        to_timestamp(o.delivery_due_time) as delivery_due_time,
        o.delivery_option_id,
        to_timestamp(o.delivery_option_required_delivery_time) as delivery_option_required_delivery_time,
        to_timestamp(o.delivery_sla_time) as delivery_sla_time,
        o.delivery_type,
        o.fast_delivery_program,
        to_timestamp(o.fast_dispatch_sla_time) as fast_dispatch_sla_time,
        o.has_updated_recipient_address,
        o.is_buyer_request_cancel,
        o.is_cod,
        o.is_on_hold_order,
        o.is_replacement_order,
        o.is_sample_order,
        o.need_upload_invoice,
        o.payment_method_name,
        to_timestamp(o.pick_up_cut_off_time) as pick_up_cut_off_time,
        o.replaced_order_id,
        to_timestamp(o.request_cancel_time) as request_cancel_time,
        to_timestamp(o.rts_sla_time) as rts_sla_time,
        to_timestamp(o.rts_time) as rts_time,
        o.seller_note,
        to_timestamp(o.shipping_due_time) as shipping_due_time,
        o.shipping_provider,
        o.shipping_provider_id,
        o.shipping_type,
        o.split_or_combine_tag,
        to_timestamp(o.tts_sla_time) as tts_sla_time,
        o.user_id,
        o.warehouse_id,

        -- Product information (from line_items JSON)
        parsed.value:"currency"::string as currency,
        parsed.value:"id"::int::string as line_item_id,  -- Cast to int first to normalize decimal formats
        parsed.value:"product_id"::string as product_id,
        parsed.value:"product_name"::string as product_name,
        parsed.value:"sku_id"::string as sku_id,
        parsed.value:"sku_name"::string as sku_name,
        parsed.value:"sku_image"::string as sku_image,
        parsed.value:"seller_sku"::string as seller_sku,

        -- Pricing information (from line_items JSON)
        parsed.value:"sale_price"::float as sale_price,
        parsed.value:"original_price"::float as original_price,
        parsed.value:"seller_discount"::float as seller_discount,
        parsed.value:"platform_discount"::float as platform_discount,
        parsed.value:"item_tax"[0]:"tax_amount"::float as item_tax_amount,

        -- Item status (from line_items JSON)
        parsed.value:"package_status"::string as package_status,
        parsed.value:"package_id"::string as package_id,
        to_timestamp_ltz(parsed.value:"rts_time"::number) as rts_time_ts,

        -- Daton metadata columns
        o.daton_batch_id,
        o.daton_batch_runtime,
        o.daton_user_id,
        to_timestamp(o.daton_batch_runtime/1000) as daton_batch_date,
        to_timestamp(o.update_time) as update_time,
        date_trunc('day', daton_batch_date) as ds

    from {{ source('tiktok', b.source) }} o
    cross join lateral flatten(input => parse_json(o.line_items)) as parsed

    {% if is_incremental() %}
    where to_timestamp(o.update_time) >= (
        select max_date
        from max_timestamps
        where brand = upper('{{ b.brand }}')
    )
    {% endif %}
    {% if not loop.last %}
    union all
    {% endif %}

    {% endfor %}

)

select
    flr.source_platform,
    flr.brand,
    flr.order_id,
    flr.order_tracking_number,

    -- Order details
    flr.order_delivery_time,
    flr.is_exchange_order,
    flr.order_status,
    flr.paid_time,
    flr.create_time,
    flr.delivery_option_name,
    flr.fulfillment_type,

    -- Additional order fields
    flr.auto_combine_group_id,
    flr.buyer_message,
    flr.cancel_order_sla_time,
    flr.cancel_reason,
    flr.cancel_time,
    flr.cancellation_initiator,
    flr.collection_due_time,
    flr.collection_time,
    flr.cpf,
    flr.delivery_due_time,
    flr.delivery_option_id,
    flr.delivery_option_required_delivery_time,
    flr.delivery_sla_time,
    flr.delivery_type,
    flr.fast_delivery_program,
    flr.fast_dispatch_sla_time,
    flr.has_updated_recipient_address,
    flr.is_buyer_request_cancel,
    flr.is_cod,
    flr.is_on_hold_order,
    flr.is_replacement_order,
    flr.is_sample_order,
    flr.need_upload_invoice,
    flr.payment_method_name,
    flr.pick_up_cut_off_time,
    flr.replaced_order_id,
    flr.request_cancel_time,
    flr.rts_sla_time,
    flr.rts_time,
    flr.seller_note,
    flr.shipping_due_time,
    flr.shipping_provider,
    flr.shipping_provider_id,
    flr.shipping_type,
    flr.split_or_combine_tag,
    flr.tts_sla_time,
    flr.user_id,
    flr.warehouse_id,

    -- Product information
    flr.currency,
    flr.line_item_id,
    flr.product_id,
    flr.product_name,
    flr.sku_id,
    flr.sku_name,
    flr.sku_image,
    flr.seller_sku,

    -- Pricing information
    flr.sale_price,
    flr.original_price,
    flr.seller_discount,
    flr.platform_discount,
    flr.item_tax_amount,

    -- Item status
    flr.package_status,
    flr.package_id,
    flr.rts_time_ts,

    -- Daton metadata
    flr.daton_batch_id,
    flr.daton_batch_runtime,
    flr.daton_user_id,
    flr.daton_batch_date,
    flr.update_time,
    flr.ds

from flattened_line_items_raw flr