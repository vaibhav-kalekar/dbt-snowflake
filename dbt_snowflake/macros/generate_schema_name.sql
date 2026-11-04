{#- prod: use the custom schema as-is (RAW, STAGING, MARTS ...).
    dev/ci: prefix with the developer / PR schema so nothing collides. -#}
{% macro generate_schema_name(custom_schema_name, node=none) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema | trim }}
    {%- elif target.name == 'prod' -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema | trim }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
