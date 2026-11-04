{#- Business logic lives here: banding, year, validity flag. -#}
select
    claim_id,
    patient_id,
    claim_amount,
    claim_date,
    extract(year from claim_date) as claim_year,
    {{ claim_category('claim_amount') }} as claim_category,
    (claim_amount is not null and claim_date is not null) as is_valid_claim,
    loaded_at
from {{ ref('stg_healthcare__claims') }}
