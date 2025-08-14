{% macro get_date_filter(date_column, start_date, end_date= None) %}
    
    {% set date_filters = [] %}
    {% if end_date %}
        {% do date_filters.append(date_column ~ " >= '" ~ start_date ~"'") %}
        {% do date_filters.append(date_column ~ " <= '" ~ end_date ~"'") %}
    {% else %}
        {% do date_filters.append(date_column ~ " = '" ~ start_date ~"'") %}
    {%endif%}
    {% do log (date_filters, info= True ) %}
    {{ return(date_filters) }}
{%endmacro%}