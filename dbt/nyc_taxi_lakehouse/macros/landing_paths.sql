{% macro dataset_month_value() -%}
  {% set year_value = var('year') | int %}
  {% set month_value = var('month') | int %}
  {{ return(year_value ~ "-" ~ "{:02d}".format(month_value)) }}
{%- endmacro %}

{% macro lakehouse_root() -%}
  {% set explicit_root = env_var('LAKEHOUSE_ROOT', '') | trim %}
  {% if explicit_root %}
    {{ return(explicit_root.rstrip('/')) }}
  {% endif %}

  {% set bucket_uri = env_var('LAKEHOUSE_BUCKET_URI', 's3://nyc-data-platform-test') | trim %}
  {% set environment = env_var('LAKEHOUSE_ENV', 'test') | trim('/') %}
  {{ return(bucket_uri.rstrip('/') ~ '/' ~ environment) }}
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

  {% set root = var('landing_root', '') | trim %}
  {% set dataset_month = dataset_month_value() %}
  {% set year_value = var('year') | int %}
  {% set month_value = var('month') | int %}
  {% set month_token = "{:02d}".format(month_value) %}
  {% set normalized_root = spark_compatible_uri(root if root else lakehouse_root()).rstrip('/') %}

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

{% macro reference_landing_path(reference_name, file_name) -%}
  {% set root = var('landing_root', '') | trim %}
  {% set normalized_root = spark_compatible_uri(root if root else lakehouse_root()).rstrip('/') %}
  {{ return(
      normalized_root
      ~ "/landing/reference/"
      ~ reference_name
      ~ "/"
      ~ file_name
  ) }}
{%- endmacro %}
