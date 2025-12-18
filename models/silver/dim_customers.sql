{{ config(materialized='table') }}
with b as ( select * from {{ ref('bronze__customers') }} )
select
  customer_id,
  {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_sk,
  initcap(first_name) as first_name,
  initcap(last_name) as last_name,
  email,
  signup_ts,
  country,
  current_timestamp() as loaded_at
from b;