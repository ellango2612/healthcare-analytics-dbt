{{
    config(
        materialized='table'
    )
}}

with members as (
    select distinct region from {{ ref('stg_members') }}
),

final as (
    select
        region as region_code,
        region as region_name,
        current_timestamp as _updated_at
    from members
)

select * from final
