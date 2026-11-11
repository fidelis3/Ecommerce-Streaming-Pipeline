{# Use the custom schema name exactly (STAGING), instead of dbt's default
   "<target schema>_<custom schema>" (which would give ANALYTICS_STAGING). #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
