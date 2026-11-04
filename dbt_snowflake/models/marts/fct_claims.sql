{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='claim_id',
        contract={'enforced': true}
    )
}}

select
    claims.claim_id,
    claims.patient_id,
    claims.claim_amount,
    claims.claim_date,
    cast(claims.claim_year as integer) as claim_year,
    claims.claim_category,
    claims.loaded_at
from {{ ref('int_claims__enriched') }} as claims
where
    claims.is_valid_claim
    {% if is_incremental() %}
        {#- 3-day lookback re-processes late-arriving rows; merge on claim_id keeps it idempotent. -#}
        and claims.loaded_at > (
            select dateadd('day', -3, max(existing.loaded_at))
            from {{ this }} as existing
        )
    {% endif %}
