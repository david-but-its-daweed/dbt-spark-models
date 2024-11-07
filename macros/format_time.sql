{% macro format_time(field) -%}
    CASE 
        WHEN {{ field }} is not null then CONCAT_WS(':', LPAD(CAST({{ field }} AS INT), 2, '0'), LPAD(CAST(({{ field }} % 1) * 60 AS INT), 2, '0'))
        ELSE NULL
    END{% endmacro %}
