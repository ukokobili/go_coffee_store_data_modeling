{% macro clean_store_name(store_name) %}
    regexp_replace({{ store_name }}, '@', '')
{% endmacro %}