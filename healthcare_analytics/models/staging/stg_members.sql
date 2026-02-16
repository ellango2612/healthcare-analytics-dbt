with source as (
    select * from {{ source('raw', 'members') }}
),

cleaned as (
    select
        member_id,
        first_name,
        last_name,
        lower(trim(region)) as region,
        cast(signup_date as date) as signup_date,
        current_timestamp as _loaded_at
    from source
)

select * from cleaned
