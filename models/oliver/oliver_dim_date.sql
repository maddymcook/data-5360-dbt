{{ config(materialized='view') }}

with order_dates as (

    select distinct
        order_date as date_key
    from {{ source('oliver', 'orders') }}

),

certification_dates as (

    select distinct
        parse_json(certification_json):certification_awarded_date::date as date_key
    from {{ source('oliver', 'employee_certifications') }}

),

all_dates as (

    select date_key from order_dates
    union
    select date_key from certification_dates

)

select distinct
    date_key,
    year(date_key) as year,
    month(date_key) as month,
    day(date_key) as day
from all_dates