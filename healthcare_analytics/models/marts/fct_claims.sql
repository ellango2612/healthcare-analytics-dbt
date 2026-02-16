{{
    config(
        materialized='incremental',
        unique_key='claim_id'
    )
}}

with claims as (
    select * from {{ ref('stg_claims') }}
),

members as (
    select 
        member_id,
        region
    from {{ ref('stg_members') }}
),

final as (
    select
        c.claim_id,
        c.member_id,
        m.region as region_code,
        c.claim_date,
        c.claim_amount,
        c.claim_type,
        
        -- Extract date parts for time-based analysis
        extract(year from c.claim_date) as claim_year,
        extract(month from c.claim_date) as claim_month,
        extract(quarter from c.claim_date) as claim_quarter,
        
        current_timestamp as _updated_at
    from claims c
    left join members m on c.member_id = m.member_id
    
    {% if is_incremental() %}
        -- Only process new claims
        where c.claim_date > (select max(claim_date) from {{ this }})
    {% endif %}
)

select * from final
