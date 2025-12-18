{{ config(materialized='table') }}
select
  product_id,
  {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_sk,
  sku, 
  name,
  category,
  price,
  created_ts,
  current_timestamp() as loaded_at
from {{ ref('bronze__products') }};
