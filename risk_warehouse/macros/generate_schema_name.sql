{#
    Use a model's custom schema as-is (staging, intermediate, core, ml) instead of dbt's default
    "<target_schema>_<custom_schema>" (main_staging, ...). Models without one use the target schema.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
