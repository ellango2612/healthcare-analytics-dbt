{{
    config(
        materialized='table'
    )
}}

with members as (
    select * from {{ ref('stg_members') }}
),

final as (
    select
        member_id,
        first_name,
        last_name,
        first_name || ' ' || last_name as full_name,
        region,
        signup_date,
        current_timestamp as _updated_at
    from members
)

select * from final
