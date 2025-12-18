{{ config(materialized='table') }}
select
  date(order_ts) as sales_date,
  sum(total_amount) as total_sales,
  count(distinct order_id) as num_orders
from {{ ref('fct_orders') }}
group by date(order_ts)
order by sales_date;
