{#- Cleanup for per-PR CI schemas. Refuses to run outside the ci target.
    Usage: dbt run-operation drop_ci_schemas --target ci -#}
{% macro drop_ci_schemas() %}
    {% if target.name != 'ci' or not target.schema.startswith('ci_pr_') %}
        {{ exceptions.raise_compiler_error("drop_ci_schemas only runs on the ci target with a ci_pr_* schema") }}
    {% endif %}
    {% set find_schemas %}
        select schema_name
        from {{ target.database }}.information_schema.schemata
        where startswith(upper(schema_name), upper('{{ target.schema }}'))
    {% endset %}
    {% for row in run_query(find_schemas).rows %}
        {% do log('Dropping ' ~ target.database ~ '.' ~ row[0], info=true) %}
        {% do run_query('drop schema if exists ' ~ target.database ~ '.' ~ row[0] ~ ' cascade') %}
    {% endfor %}
{% endmacro %}
