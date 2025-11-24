{% macro coc_to_timestamp(coc_timestamp_field) %}
    CASE 
        WHEN {{ coc_timestamp_field }} IS NOT NULL THEN 
            TRY_TO_TIMESTAMP(
                REGEXP_REPLACE({{ coc_timestamp_field }}::VARCHAR, '^(\\d{4})(\\d{2})(\\d{2})T(\\d{2})(\\d{2})(\\d{2})\\.\\d+Z$', '\\1-\\2-\\3 \\4:\\5:\\6'),
                'YYYY-MM-DD HH24:MI:SS'
            )
        ELSE NULL
    END
{% endmacro %}