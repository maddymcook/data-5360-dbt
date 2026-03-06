{{ config(materialized='view') }}

select distinct
  order_date as date_key,
  year(order_date) as year,
  month(order_date) as month,
  day(order_date) as day
from {{ source('oliver', 'orders') }}