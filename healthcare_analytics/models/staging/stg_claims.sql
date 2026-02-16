with source as (
    select * from {{ source('raw', 'claims') }}
),

cleaned as (
    select
        claim_id,
        member_id,
        cast(claim_date as date) as claim_date,
        cast(claim_amount as decimal(10,2)) as claim_amount,
        lower(trim(claim_type)) as claim_type,
        current_timestamp as _loaded_at
    from source
)

select * from cleaned
