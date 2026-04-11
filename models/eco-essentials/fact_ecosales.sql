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
        ol.ORDER_LINE_ID,
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

),

fact_with_keys as (

    select
        {{ dbt_utils.generate_surrogate_key(['jd.ORDER_LINE_ID']) }} as sales_key,

        dc.customer_key,
        dp.product_key,
        dca.campaign_key,
        dd.date_key,

        jd.ORDER_LINE_ID,
        jd.ORDER_ID,
        jd.CUSTOMER_ID,
        jd.CAMPAIGN_ID,
        jd.PRODUCT_ID,
        jd.ORDER_DATE,
        jd.QUANTITY,
        jd.DISCOUNT,
        jd.PROMOTIONAL_CAMPAIGN,
        jd.PRICE_AFTER_DISCOUNT,
        jd.SALES_AMOUNT

    from joined_data jd
    left join {{ ref('dim_ecocustomer') }} dc
        on jd.CUSTOMER_ID = dc.customer_id
    left join {{ ref('dim_product') }} dp
        on jd.PRODUCT_ID = dp.product_id
    left join {{ ref('dim_campaign') }} dca
        on jd.CAMPAIGN_ID = dca.campaign_id
    left join {{ ref('dim_ecodate') }} dd
        on jd.ORDER_DATE = dd.date_day

)

select
    sales_key,
    customer_key,
    product_key,
    campaign_key,
    date_key,
    ORDER_LINE_ID,
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
from fact_with_keys