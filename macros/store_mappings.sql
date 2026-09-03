{% macro shopify_legacy_order_sources() %}
{{ return([
    {'brand': 'store_a', 'source': 'store_a_shopifyv3_orders'},
    {'brand': 'store_b', 'source': 'store_b_shopifyv2_orders'},
    {'brand': 'store_c', 'source': 'store_c_shopifyv2_orders'},
    {'brand': 'store_d', 'source': 'store_d_shopifyv2_orders'},
    {'brand': 'store_e', 'source': 'store_e_shopifyv2_orders'},
    {'brand': 'store_f', 'source': 'STORE_F_SHOPIFY_V2_ORDERS'}
]) }}
{% endmacro %}

{% macro shopify_replacement_order_sources() %}
{{ return([
    {'brand': 'store_a', 'source': 'ORDERS'},
    {'brand': 'store_c', 'source': 'ORDERS'},
    {'brand': 'store_e', 'source': 'ORDERS'},
    {'brand': 'store_f', 'source': 'ORDERS'},
    {'brand': 'store_b', 'source': 'ORDERS'},
    {'brand': 'store_d', 'source': 'ORDERS'}
]) }}
{% endmacro %}

{% macro shopify_legacy_refund_sources() %}
{{ return([
    {'brand': 'store_a', 'source': 'store_a_shopifyv3_refunds'},
    {'brand': 'store_b', 'source': 'store_b_shopifyv2_refunds'},
    {'brand': 'store_c', 'source': 'store_c_shopifyv2_refunds'},
    {'brand': 'store_d', 'source': 'store_d_shopifyv2_refunds'},
    {'brand': 'store_e', 'source': 'store_e_shopifyv2_refunds'},
    {'brand': 'store_f', 'source': 'STORE_F_SHOPIFY_V2_REFUNDS'}
]) }}
{% endmacro %}

{% macro shopify_replacement_refund_sources() %}
{{ return([
    {'brand': 'store_a', 'source': 'ORDER_REFUNDS'},
    {'brand': 'store_c', 'source': 'ORDER_REFUNDS'},
    {'brand': 'store_e', 'source': 'ORDER_REFUNDS'},
    {'brand': 'store_f', 'source': 'ORDER_REFUNDS'},
    {'brand': 'store_b', 'source': 'ORDER_REFUNDS'},
    {'brand': 'store_d', 'source': 'ORDER_REFUNDS'}
]) }}
{% endmacro %}

{% macro tiktok_order_sources() %}
{{ return([
    {'brand': 'store_a', 'source': 'store_a_tiktok_orders'},
    {'brand': 'store_c', 'source': 'store_c_tiktok_orders'},
    {'brand': 'store_d', 'source': 'store_d_tiktok_orders'}
]) }}
{% endmacro %}

{% macro tiktok_return_sources() %}
{{ return([
    {'brand': 'STORE_A', 'source': 'store_a_tiktok_returns'},
    {'brand': 'STORE_C', 'source': 'store_c_tiktok_returns'},
    {'brand': 'STORE_D', 'source': 'store_d_tiktok_returns'}
]) }}
{% endmacro %}

{% macro tiktok_statement_sources() %}
{{ return([
    {'brand': 'store_a', 'source': 'store_a_tiktok_statements'},
    {'brand': 'store_c', 'source': 'store_c_tiktok_statements'},
    {'brand': 'store_d', 'source': 'store_d_tiktok_statements'}
]) }}
{% endmacro %}

{% macro store_brand_mapping_sql() %}
select 'STORE_A' as brand, 'Store A' as brand_full
union all select 'STORE_B', 'Store B'
union all select 'STORE_C', 'Store C'
union all select 'STORE_F', 'Store F'
union all select 'STORE_D', 'Store D'
union all select 'STORE_E', 'Store E'
union all select 'STORE_G', 'Store G'
{% endmacro %}

{% macro migrated_shopify_store_codes() %}
'STORE_A', 'STORE_C', 'STORE_E', 'STORE_F', 'STORE_B', 'STORE_D'
{% endmacro %}
