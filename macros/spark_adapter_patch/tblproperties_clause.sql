{% macro tblproperties_clause() %}
  {%- set tblproperties = config.get('tblproperties', {}) -%}
  {%- set tblproperties2 = var("tblproperties") or {} -%}

  {%- if tblproperties or tblproperties2 %}
    TBLPROPERTIES (
    {%- for prop, val in tblproperties.items() %}
      {%- set escaped_value = val | replace('\'', '\\\'') %}
      '{{ prop }}'='{{ escaped_value }}'
      {%- if not loop.last -%},{%- endif -%}
    {% endfor %}

    {%- for prop, val in tblproperties2.items() %}
      {%- set escaped_value = val | replace('\'', '\\\'') %}
      '{{ prop }}'='{{ escaped_value }}'
      {%- if not loop.last -%},{%- endif -%}
    {% endfor %}
    )
  {%- endif %}
{% endmacro %}