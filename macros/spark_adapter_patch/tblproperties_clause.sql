{% macro tblproperties_clause() %}
  {%- set tblproperties = config.get('tblproperties', {}) -%}

  {%- if tblproperties %}
    TBLPROPERTIES (
    {%- for prop, val in tblproperties.items() %}
      {%- set escaped_value = val | replace('\'', '\\\'') %}
      '{{ prop }}'='{{ escaped_value }}'
      {%- if not loop.last -%},{%- endif -%}
    {% endfor %}
    )
  {%- endif %}
{% endmacro %}