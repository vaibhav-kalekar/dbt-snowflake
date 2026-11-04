{{ config(contract={'enforced': true}) }}

select
    cast({{ dbt_utils.generate_surrogate_key(['patient_id', 'dbt_valid_from']) }} as varchar) as patient_key,
    patient_id,
    date_of_birth,
    gender,
    state,
    cast(dbt_valid_from as timestamp_ntz) as valid_from,
    cast(dbt_valid_to as timestamp_ntz) as valid_to,
    (dbt_valid_to is null) as is_current
from {{ ref('snap_patients') }}
