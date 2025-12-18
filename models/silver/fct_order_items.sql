{{ config(materialized='table') }}
with oi as ( select * from {{ ref('bronze__order_items') }} ),
prod as ( select product_id, product_sk from {{ ref('dim_products') }} )
select
  oi.order_item_id, oi.order_id, oi.product_id, prod.product_sk,
  oi.qty, oi.price, (oi.qty*oi.price) as line_total,
  current_timestamp() as loaded_at
from oi left join prod on oi.product_id = prod.product_id;
