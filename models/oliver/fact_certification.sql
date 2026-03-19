with certifications as (

    select * 
    from {{ ref('stg_employee_certifications') }}

),

employee as (

    select * 
    from {{ ref('oliver_dim_employee') }}

),

date as (

    select * 
    from {{ ref('oliver_dim_date') }}

),

final as (

    select
        e.employee_id,
        d.date_key,
        c.certification_name,
        c.certification_cost

    from certifications c

    left join employee e
        on c.employee_id = e.employee_id

    left join date d
        on c.certification_awarded_date = d.date_key

)

select * from final