with source as (
    select * from {{ source('raw', 'enrollments') }}
),

cleaned as (
    select
        enrollment_id,
        member_id,
        plan_id,
        cast(start_date as date) as start_date,
        cast(end_date as date) as end_date,
        lower(trim(status)) as status,
        current_timestamp as _loaded_at
    from source
)

select * from cleaned
