{{ config(materialized='incremental', unique_key='order_id', file_format='delta') }}
with raw as ( select * from {{source('landing', 'orders') }} )
select
  cast(order_id as int) as order_id,
  cast(customer_id as int) as customer_id,
  to_timestamp(order_ts) as order_ts,
  status,
  cast(total_amount as decimal(12,2)) as total_amount,
  current_timestamp() as ingestion_ts
from raw
{% if is_incremental() %}
where order_ts >= (select max(order_ts) from {{ this }})
{% endif %};
