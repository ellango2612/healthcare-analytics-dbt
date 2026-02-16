{{
    config(
        materialized='table'
    )
}}

with claims as (
    select * from {{ ref('fct_claims') }}
),

enrollments as (
    select * from {{ ref('fct_enrollments') }}
),

members as (
    select * from {{ ref('dim_members') }}
),

-- Aggregate claims by member and month
member_monthly_claims as (
    select
        member_id,
        claim_year,
        claim_month,
        count(*) as claim_count,
        sum(claim_amount) as total_claim_amount,
        avg(claim_amount) as avg_claim_amount
    from claims
    group by member_id, claim_year, claim_month
),

-- Get active enrollments by member
member_enrollments as (
    select
        member_id,
        count(*) as total_enrollments,
        sum(is_active) as active_enrollments,
        sum(is_churned) as churned_enrollments
    from enrollments
    group by member_id
),

final as (
    select
        m.member_id,
        m.full_name,
        m.region,
        
        -- Enrollment metrics
        coalesce(e.total_enrollments, 0) as total_enrollments,
        coalesce(e.active_enrollments, 0) as active_enrollments,
        coalesce(e.churned_enrollments, 0) as churned_enrollments,
        
        -- Claims metrics (lifetime)
        coalesce(sum(c.claim_count), 0) as lifetime_claims,
        coalesce(sum(c.total_claim_amount), 0) as lifetime_claim_amount,
        
        -- Average monthly metrics
        case 
            when count(distinct c.claim_year || '-' || c.claim_month) > 0
            then sum(c.claim_count) / count(distinct c.claim_year || '-' || c.claim_month)
            else 0
        end as avg_monthly_claims,
        
        case 
            when count(distinct c.claim_year || '-' || c.claim_month) > 0
            then sum(c.total_claim_amount) / count(distinct c.claim_year || '-' || c.claim_month)
            else 0
        end as avg_monthly_claim_amount,
        
        current_timestamp as _updated_at
        
    from members m
    left join member_enrollments e on m.member_id = e.member_id
    left join member_monthly_claims c on m.member_id = c.member_id
    group by 
        m.member_id, 
        m.full_name, 
        m.region,
        e.total_enrollments,
        e.active_enrollments,
        e.churned_enrollments
)

select * from final
