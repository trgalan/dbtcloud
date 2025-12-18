{{ config(materialized='table') }}
with dates as ( select distinct date(order_ts) as d from {{ ref('bronze__orders') }} )
select
  d as full_date, year(d) as year, month(d) as month, day(d) as day, dayofweek(d) as day_of_week
from dates;
