{{ config(
    materialized = 'table',
    schema = 'dw_ecoessentials'
) }}

with orders as (

    select
        ORDER_ID,
        CUSTOMER_ID,
        ORDER_TIMESTAMP
    from {{ source('ecoessentials_landing', 'orders') }}

),

order_lines as (

    select
        ORDER_LINE_ID,
        ORDER_ID,
        PRODUCT_ID,
        CAMPAIGN_ID,
        QUANTITY,
        DISCOUNT,
        PROMOTIONAL_CAMPAIGN,
        PRICE_AFTER_DISCOUNT
    from {{ source('ecoessentials_landing', 'order_line') }}

),

joined_data as (

    select
        o.ORDER_ID,
        o.CUSTOMER_ID,
        ol.CAMPAIGN_ID,
        ol.PRODUCT_ID,
        cast(o.ORDER_TIMESTAMP as date) as ORDER_DATE,
        ol.QUANTITY,
        ol.DISCOUNT,
        ol.PROMOTIONAL_CAMPAIGN,
        ol.PRICE_AFTER_DISCOUNT,
        (ol.QUANTITY * ol.PRICE_AFTER_DISCOUNT) as SALES_AMOUNT
    from orders o
    inner join order_lines ol
        on o.ORDER_ID = ol.ORDER_ID

)

select
    {{ dbt_utils.generate_surrogate_key([
        'ORDER_ID',
        'PRODUCT_ID',
        'CUSTOMER_ID',
        'CAMPAIGN_ID',
        'ORDER_DATE'
    ]) }} as sales_key,

    {{ dbt_utils.generate_surrogate_key(['CUSTOMER_ID']) }} as customer_key,
    {{ dbt_utils.generate_surrogate_key(['PRODUCT_ID']) }} as product_key,
    {{ dbt_utils.generate_surrogate_key(['CAMPAIGN_ID']) }} as campaign_key,
    {{ dbt_utils.generate_surrogate_key(['ORDER_DATE']) }} as date_key,

    ORDER_ID,
    CUSTOMER_ID,
    CAMPAIGN_ID,
    PRODUCT_ID,
    ORDER_DATE,
    QUANTITY,
    DISCOUNT,
    PROMOTIONAL_CAMPAIGN,
    PRICE_AFTER_DISCOUNT,
    SALES_AMOUNT

from joined_data