{{
  config(
    unique_key="dataset_month",
    on_schema_change="sync_all_columns"
  )
}}

{% set landing_path = landing_month_path("green") %}

select
  *,
  cast({{ var("year") }} as int) as source_year,
  cast({{ var("month") }} as int) as source_month,
  cast('{{ dataset_month_value() }}' as string) as dataset_month,
  input_file_name() as source_file,
  current_timestamp() as ingested_at
from parquet.`{{ landing_path }}`
