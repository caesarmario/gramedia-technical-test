-- ##############################################
-- Gramedia Digital - Data Engineer Take Home Test
-- macros for category noramlization reusable
-- Mario Caesar // caesarmario87@gmail.com
-- ##############################################

{% macro clean_category(col) -%}
lower(trim({{ col }}))
{%- endmacro %}
