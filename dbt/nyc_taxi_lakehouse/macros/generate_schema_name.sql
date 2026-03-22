{% macro generate_schema_name(custom_schema_name, node) -%}
  {% if custom_schema_name is none %}
    {{ return(target.schema) }}
  {% endif %}
  {{ return(custom_schema_name | trim) }}
{%- endmacro %}
