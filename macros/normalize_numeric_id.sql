{% macro normalize_numeric_id(expression) %}
    coalesce(
        try_to_number({{ expression }}::string)::integer::string,
        {{ expression }}::string
    )
{% endmacro %}
