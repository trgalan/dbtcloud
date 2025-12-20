{{ config(materialized='table', file_format='delta') }}
with raw as ( select * from {{ source('landing', 'products') }} )
select
  cast(product_id as int) as product_id,
  sku, name, category,
  cast(price as decimal(10,2)) as price,
  to_timestamp(created_ts) as created_ts,
  current_timestamp() as ingestion_ts
from raw;
