{{ config(materialized='incremental', unique_key='order_item_id', file_format='delta') }}
with raw as ( select * from {{ ref('order_items') }} )
select
  cast(order_item_id as int) as order_item_id,
  cast(order_id as int) as order_id,
  cast(product_id as int) as product_id,
  cast(qty as int) as qty,
  cast(price as decimal(12,2)) as price,
  current_timestamp() as ingestion_ts
from raw
{% if is_incremental() %}
where order_item_id > (select max(order_item_id) from {{ this }})
{% endif %};
