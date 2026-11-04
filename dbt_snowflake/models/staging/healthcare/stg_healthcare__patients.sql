select
    cast(patient_id as varchar) as patient_id,
    cast(date_of_birth as date) as date_of_birth,
    cast(gender as varchar) as gender,
    cast(state as varchar) as state,
    cast(loaded_at as timestamp_ntz) as loaded_at
from {{ source('healthcare', 'raw_patients') }}
