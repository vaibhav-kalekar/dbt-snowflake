{#- Staging: rename, cast, nothing else. No filtering and no business rules here. -#}
select
    cast(claim_id as varchar) as claim_id,
    cast(patient_id as varchar) as patient_id,
    cast(claim_amount as number(18, 2)) as claim_amount,
    cast(claim_date as date) as claim_date,
    cast(loaded_at as timestamp_ntz) as loaded_at
from {{ source('healthcare', 'raw_claims') }}
