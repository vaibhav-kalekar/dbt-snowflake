{#- Quarantine: rows excluded from fct_claims, with the reason. Review, don't ignore. -#}
select
    claim_id,
    patient_id,
    claim_amount,
    claim_date,
    case
        when claim_amount is null then 'missing_claim_amount'
        when claim_date is null then 'missing_claim_date'
    end as rejection_reason,
    loaded_at
from {{ ref('int_claims__enriched') }}
where not is_valid_claim
