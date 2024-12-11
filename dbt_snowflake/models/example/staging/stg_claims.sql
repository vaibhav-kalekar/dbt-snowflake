{{ config(materialized='view') }}

SELECT
    claim_id,
    patient_id,
    claim_amount,
    claim_date,
    EXTRACT(YEAR FROM claim_date) as claim_year,
    CASE 
        WHEN claim_amount < 1000 THEN 'Low'
        WHEN claim_amount BETWEEN 1000 AND 5000 THEN 'Medium'
        ELSE 'High'
    END AS claim_category
FROM {{ source('healthcare', 'raw_claims') }}
WHERE claim_amount IS NOT NULL