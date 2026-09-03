{% macro signed_amount(amount_expression, row_type_expression) %}
    case
        when lower(coalesce({{ row_type_expression }}, 'sale')) = 'cancellation'
            then -abs({{ amount_expression }})
        else abs({{ amount_expression }})
    end
{% endmacro %}

{#
  Convert a native-currency amount using a joined monthly FX relation.
  The FX relation must expose one column per supported currency and rates
  expressed as native-currency units per reporting-currency unit.
#}
{% macro convert_currency(amount_expression, currency_expression, fx_alias='fx', reporting_currency='USD') %}
    case upper({{ currency_expression }})
        when upper('{{ reporting_currency }}') then {{ amount_expression }}
        when 'EUR' then {{ amount_expression }} / nullif({{ fx_alias }}.eur, 0)
        when 'GBP' then {{ amount_expression }} / nullif({{ fx_alias }}.gbp, 0)
        when 'CAD' then {{ amount_expression }} / nullif({{ fx_alias }}.cad, 0)
        else null
    end
{% endmacro %}
