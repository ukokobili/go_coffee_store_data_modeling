{% macro limit_in_dev(sql) %}
    {%- if target.name == "dev" -%} {{ sql }} limit {{ var("sample_size", 1000) }}
    {%- else -%} {{ sql }}
    {%- endif -%}
{% endmacro %}