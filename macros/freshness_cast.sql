{% macro get_loaded_at_field(source, table, column_name) %}
  case 
    when '{{ column_name }}' = 'DATON_BATCH_RUNTIME' 
    then 'TO_TIMESTAMP("DATON_BATCH_RUNTIME" / 1000)'
    else '"' ~ column_name ~ '"'
  end
{% endmacro %}
