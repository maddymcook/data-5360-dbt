{{ config(materialized='table') }}

select
  o.order_id,
  o.order_date,
  o.store_id,
  o.customer_id,
  o.employee_id,
  ol.order_line_id,
  ol.product_id,
  ol.quantity,
  ol.unit_price,
  (ol.quantity * ol.unit_price) as line_amount,
  o.total_amount as order_total_amount
from {{ source('oliver', 'orders') }} o
join {{ source('oliver', 'orderline') }} ol
  on o.order_id = ol.order_id