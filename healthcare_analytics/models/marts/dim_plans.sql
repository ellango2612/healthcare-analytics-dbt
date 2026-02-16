{{
    config(
        materialized='table'
    )
}}

with plans as (
    select * from {{ ref('stg_plans') }}
),

final as (
    select
        plan_id,
        plan_name,
        metal_tier,
        monthly_premium,
        
        -- Add descriptive tier order for sorting
        case metal_tier
            when 'bronze' then 1
            when 'silver' then 2
            when 'gold' then 3
            when 'platinum' then 4
        end as tier_order,
        
        current_timestamp as _updated_at
    from plans
)

select * from final
