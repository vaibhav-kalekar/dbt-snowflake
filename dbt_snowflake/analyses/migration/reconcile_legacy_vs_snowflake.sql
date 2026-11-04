{#- Parallel-run reconciliation: legacy (Teradata) extract vs the Snowflake fact.
    Compile with `dbt compile -s reconcile_legacy_vs_snowflake`, then run the SQL in Snowflake.
    Expect: CLM-0006 amount mismatch, CLM-0010 missing from legacy. -#}
{{
    audit_helper.compare_queries(
        a_query="select claim_id, patient_id, claim_amount, claim_date from " ~ ref('legacy_claims_extract'),
        b_query="select claim_id, patient_id, claim_amount, claim_date from " ~ ref('fct_claims'),
        primary_key='claim_id'
    )
}}
