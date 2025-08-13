{% macro test_counts(
    source_table,
    target_table,
    date_column=None
)
%}

    {# setting up dates #}
    {% set start_date = var(start_date, none) %}
    {% set end_date = var(end_date, none) %}
    
    {# check for tables existence #}
    {% set source_tbl = adapter.get_relation(
        database = target.database,
        schema = target.schema,
        identifier = source_table
    )%}
    
    {% set target_tbl = adapter.get_relation(
        database = target.database,
        schema = target.schema,
        identifier = target_table
    )%}
    
    {% if source_tbl and target_tbl %}
        {% set filters = " where 1=1 " %}
        {% if date_column and (start_date or end_date) %}
            date_filters = []
            {% if end_date %}
                {% do date_filters.append(date_column ~ " >= '" ~ start_date ~"'") %}
                {% do date_filters.append(date_column ~ " <= '" ~ end_date ~"'") %}
            {% else %}
                {% do date_filters.append(date_column ~ " = '" ~ start_date ~"'") %}
            {%endif%}
        {% set filters = filters ~ date_filters | join(" and ") %}
        {% endif %}

        with src as (
        select count(*) as cnt
            from {{ source_tbl }}
            {{ filters }}
        ),
        tgt as (
            select count(*) as cnt
                from {{ target_tbl }}
                    {{ filters }}
        )
        select src.cnt,
                tgt.cnt
            from src,tgt 
        where src.cnt != tgt.cnt
    {% else %}
        {# skip the test#}
        select null where false
    {% endif %}
{% endmacro %}
        