{% snapshot snap_patients %}
{{
    config(
        target_schema=generate_schema_name('snapshots'),
        unique_key='patient_id',
        strategy='check',
        check_cols=['date_of_birth', 'gender', 'state']
    )
}}
select * from {{ ref('stg_healthcare__patients') }}
{% endsnapshot %}
