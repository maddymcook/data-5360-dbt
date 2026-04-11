{{ config(
    materialized = 'table',
    schema = 'dw_ecoessentials'
) }}

with customer_base as (

    select
        customer_id,
        customer_first_name,
        customer_last_name,
        customer_phone,
        customer_email,
        customer_address,
        customer_city,
        customer_state,
        customer_zip,
        customer_country
    from {{ source('ecoessentials_landing', 'customer') }}

),

subscriber_base as (

    select
        try_cast(CUSTOMERID as number) as customer_id,
        max(SUBSCRIBERID) as subscriber_id,
        max(SUBSCRIBEREMAIL) as subscriber_email,
        max(SUBSCRIBERFIRSTNAME) as subscriber_first_name,
        max(SUBSCRIBERLASTNAME) as subscriber_last_name
    from {{ source('ecoessentials_landing', 'marketingemails') }}
    where try_cast(CUSTOMERID as number) is not null
    group by try_cast(CUSTOMERID as number)

)

select
    {{ dbt_utils.generate_surrogate_key(['coalesce(c.customer_id, s.customer_id)']) }} as customer_key,
    coalesce(c.customer_id, s.customer_id) as customer_id,
    c.customer_first_name,
    c.customer_last_name,
    c.customer_phone,
    c.customer_email,
    c.customer_address,
    c.customer_city,
    c.customer_state,
    c.customer_zip,
    c.customer_country,
    s.subscriber_id,
    s.subscriber_email,
    s.subscriber_first_name,
    s.subscriber_last_name
from customer_base c
full outer join subscriber_base s
    on c.customer_id = s.customer_id
where coalesce(c.customer_id, s.customer_id) is not null