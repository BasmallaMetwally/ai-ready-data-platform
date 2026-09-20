{% macro generate_schema_name(custom_schema_name, node) -%}
    {#-
      Default dbt behaviour prefixes every schema with the target schema, giving
      you `analytics_analytics`. In prod we want the clean name; in dev we keep
      the prefix so developers never collide with each other.
    -#}
    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- elif target.name == 'prod' -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
