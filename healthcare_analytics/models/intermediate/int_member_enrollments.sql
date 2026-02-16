with members as (
    select * from {{ ref('stg_members') }}
),

enrollments as (
    select * from {{ ref('stg_enrollments') }}
),

plans as (
    select * from {{ ref('stg_plans') }}
),

joined as (
    select
        e.enrollment_id,
        e.member_id,
        m.first_name,
        m.last_name,
        m.region,
        m.signup_date,
        e.plan_id,
        p.plan_name,
        p.metal_tier,
        p.monthly_premium,
        e.start_date as enrollment_start_date,
        e.end_date as enrollment_end_date,
        e.status as enrollment_status,
        
        -- Calculate enrollment duration in days
        datediff(day, e.start_date, e.end_date) as enrollment_duration_days,
        
        -- Flag for active enrollment
        case 
            when e.status = 'active' then 1 
            else 0 
        end as is_active,
        
        -- Flag for churned enrollment
        case 
            when e.status = 'churned' then 1 
            else 0 
        end as is_churned
        
    from enrollments e
    inner join members m on e.member_id = m.member_id
    inner join plans p on e.plan_id = p.plan_id
)

select * from joined
