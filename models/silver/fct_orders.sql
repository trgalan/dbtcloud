{{ config(materialized='table') }}
with o as ( 
    select * from {{ ref('bronze__orders') }} 
    ),
cust as ( 
    select customer_id, customer_sk from {{ ref('dim_customers') }} 
    )
select o.order_id, o.customer_id, cust.customer_sk, o.order_ts, o.status, o.total_amount
from o
left join cust on o.customer_id = cust.customer_id;
