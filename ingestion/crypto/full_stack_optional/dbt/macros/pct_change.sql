{% macro pct_change(current_column, previous_column) %}
    case
        when {{ previous_column }} is null or {{ previous_column }} = 0 then null
        else ({{ current_column }} - {{ previous_column }}) / {{ previous_column }}
    end
{% endmacro %}
