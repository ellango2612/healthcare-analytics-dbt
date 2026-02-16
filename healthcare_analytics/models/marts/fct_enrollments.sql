{{
    config(
        materialized='table'
    )
}}

with enrollments as (
    select * from {{ ref('int_member_enrollments') }}
),

final as (
    select
        enrollment_id,
        member_id,
        plan_id,
        region as region_code,
        enrollment_start_date,
        enrollment_end_date,
        enrollment_status,
        enrollment_duration_days,
        monthly_premium,
        
        -- Calculate total premium paid (approximate)
        round(
            monthly_premium * (enrollment_duration_days / 30.0), 
            2
        ) as total_premium_paid,
        
        is_active,
        is_churned,
        
        current_timestamp as _updated_at
    from enrollments
)

select * from final
