{{ config(
    materialized = 'table',
    schema = 'dw_insurance'
) }}

select distinct
    md5(coalesce(agent_first_name, '') || '|' || coalesce(agent_last_name, '')) as agent_key,
    agent_first_name as firstname,
    agent_last_name as lastname,
    agent_email
from {{ ref('stg_customer_service_interactions') }}