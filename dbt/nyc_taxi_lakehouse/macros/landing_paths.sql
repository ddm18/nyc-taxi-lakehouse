{% macro dataset_month_value() -%}
  {% set year_value = var('year') | int %}
  {% set month_value = var('month') | int %}
  {{ return(year_value ~ "-" ~ "{:02d}".format(month_value)) }}
{%- endmacro %}

{% macro spark_compatible_uri(uri) -%}
  {% set normalized_uri = uri | trim %}
  {% if normalized_uri.startswith('s3://') %}
    {{ return('s3a://' ~ normalized_uri[5:]) }}
  {% endif %}
  {{ return(normalized_uri) }}
{%- endmacro %}

{% macro landing_month_path(service_name) -%}
  {% set supported = ['yellow', 'green'] %}
  {% if service_name not in supported %}
    {{ exceptions.raise_compiler_error("Unsupported service: " ~ service_name) }}
  {% endif %}

  {% set root = var('landing_root', env_var('LAKEHOUSE_S3_ROOT', 's3://nyc-data-platform-dev')) | trim %}
  {% set dataset_month = dataset_month_value() %}
  {% set year_value = var('year') | int %}
  {% set month_value = var('month') | int %}
  {% set month_token = "{:02d}".format(month_value) %}
  {% set normalized_root = spark_compatible_uri(root).rstrip('/') %}

  {{ return(
      normalized_root
      ~ "/landing/"
      ~ service_name
      ~ "/year="
      ~ year_value
      ~ "/month="
      ~ month_token
      ~ "/"
      ~ service_name
      ~ "_tripdata_"
      ~ dataset_month
      ~ ".parquet"
  ) }}
{%- endmacro %}
