{{ config(
    materialized='table',
    schema='dw_ecoessentials'
) }}

select
    {{ dbt_utils.generate_surrogate_key([
        'm.EMAILEVENTID',
        'm.EMAILID',
        'm.CAMPAIGNID',
        'm.CUSTOMERID',
        'm.EVENTTYPE',
        'm.EVENTTIMESTAMP'
    ]) }} as emailevent_key,
    e.email_key,
    ca.campaign_key,
    c.customer_key,
    ev.eventtype_key,
    d.date_key,
    m.EMAILEVENTID,
    m.EMAILID,
    m.EMAILNAME,
    TRY_CAST(m.CAMPAIGNID AS NUMBER) as CAMPAIGNID,
    m.CAMPAIGNNAME,
    TRY_CAST(m.CUSTOMERID AS NUMBER) as CUSTOMERID,
    m.SUBSCRIBERID,
    m.SUBSCRIBEREMAIL,
    m.SUBSCRIBERFIRSTNAME,
    m.SUBSCRIBERLASTNAME,
    m.SENDTIMESTAMP,
    m.EVENTTYPE,
    CAST(m.EVENTTIMESTAMP AS DATE) as EVENTDATE,
    m.EVENTTIMESTAMP,
    1 as event_count
from {{ source('ecoessentials_landing', 'marketingemails') }} m
inner join {{ ref('dim_ecocustomer') }} c
    on c.customer_id = TRY_CAST(m.CUSTOMERID AS NUMBER)
inner join {{ ref('dim_campaign') }} ca
    on ca.campaign_id = TRY_CAST(m.CAMPAIGNID AS NUMBER)
inner join {{ ref('dim_email') }} e
    on e.EMAILID = m.EMAILID
inner join {{ ref('dim_eventtype') }} ev
    on ev.EVENTTYPE = m.EVENTTYPE
inner join {{ ref('dim_ecodate') }} d
    on d.date_day = CAST(m.EVENTTIMESTAMP AS DATE)
where m.EVENTTIMESTAMP is not null