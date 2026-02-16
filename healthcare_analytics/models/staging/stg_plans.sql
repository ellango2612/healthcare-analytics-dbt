with source as (
    select * from {{ source('raw', 'plans') }}
),

cleaned as (
    select
        plan_id,
        trim(plan_name) as plan_name,
        lower(trim(metal_tier)) as metal_tier,
        cast(monthly_premium as decimal(10,2)) as monthly_premium,
        current_timestamp as _loaded_at
    from source
)

select * from cleaned
