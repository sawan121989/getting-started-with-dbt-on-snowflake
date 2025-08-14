{% test validate_src_tgt_cnt(
    model,
    source_table,
    target_table,
    date_column=None,
    target_date_column=None,
    start_dt=None,
    end_dt=None
)
%}

    {# setting up dates #}
    {% set start_date = var(start_date, start_dt) %}
    {% set end_date = var(end_date, end_dt) %}

    {% do log(start_date, info=True) %}
    {% do log(end_date, info=True) %}
    

    {# check for tables existence #}
    {#
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
    #}
    
    {% set source_tbl = source_table %}
    {% set target_tbl = target_table %}

    {% if source_tbl and target_tbl %}
        {% set filters = " where 1=1 and " %}
        {% if date_column and start_date %}
            {% set src_date_filter = get_date_filter(date_column, start_date, end_date) %}
        {% endif %}

        {% if target_date_column and start_date %}
            {% set tgt_date_filter = get_date_filter(target_date_column, start_date, end_date) %}
        {% elif date_column and start_date %}
            {% set tgt_date_filter = get_date_filter(date_column, start_date, end_date) %}
        {% endif %}

        {% set src_filters = filters ~ src_date_filter | join(" and ") %}
        {% set tgt_filters = filters ~ tgt_date_filter | join(" and ") %}

        {% do log(src_filters, info=True) %}
        {% do log(tgt_filters, info=True) %}

        with src as (
        select count(*) as cnt
            from {{ source_tbl }}
            {{ src_filters }}
        ),
        tgt as (
            select count(*) as cnt
                from {{ target_tbl }}
                {{ tgt_filters }}
        )
        select src.cnt,
                tgt.cnt
            from src,tgt 
        where src.cnt != tgt.cnt
    {% else %}
        {# skip the test#}
        {% do log('Nothing to do') %}
        select null where false
    {% endif %}
{% endtest %}
        