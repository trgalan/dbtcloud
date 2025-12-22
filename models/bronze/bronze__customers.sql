{{ config(materialized='incremental', unique_key='customer_id', file_format='delta') }}
with raw as ( select * from {{ source('landing', 'customers') }} ) 
select
  cast(customer_id as int) as customer_id,
  trim(first_name) as first_name,
  trim(last_name) as last_name,
  lower(email) as email,
  to_timestamp(signup_ts) as signup_ts,
  country,
  current_timestamp() as ingestion_ts,
  false as deleted_flag
from raw
{% if is_incremental() %}
where signup_ts >= (select max(signup_ts) from {{ this }})
{% endif %};

