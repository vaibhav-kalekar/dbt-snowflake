{#- Single definition of claim banding. Thresholds come from dbt_project.yml vars.
    NULL amounts return NULL (never silently 'High'). -#}
{% macro claim_category(amount_col) %}
    case
        when {{ amount_col }} is null then null
        when {{ amount_col }} < {{ var('claim_low_max') }} then 'Low'
        when {{ amount_col }} <= {{ var('claim_medium_max') }} then 'Medium'
        else 'High'
    end
{% endmacro %}
