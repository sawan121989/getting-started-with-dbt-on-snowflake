{% test validate_src_tgt_aggregation(
    model,
    source_table,
    target_table,
    group_columns=None,
    tgt_group_columns = None,   
    measure_columns=None, 
    tgt_measure_columns = None, 
    date_column=None,
    tgt_date_column =None,        
    start_dt=None,
    end_dt=None
) %}

    {# setting up dates #}
    {% set start_date = var(start_date, start_dt) %}
    {% set end_date = var(end_date, end_dt) %}

    {% do log(start_date, info=True)%}
    {% do log(end_date, info=True)%}

    {# check if source is sql or table #}
    {% if source_table.lower().strip().startswith("select") %}
        {% set src_input_mode = 'qry' %}
        source_tbl = source_table
    {% else %}
        {% set src_input_mode = 'tbl' %}
    {% endif %}

    {# check if target is sql or table #}
    {% if target_table.lower().strip().startswith("select") %}
        {% set tgt_input_mode = 'qry' %}
        target_tbl = target_table
    {% else %}
        {% set tgt_input_mode = 'tbl' %}
    {% endif %}

    {#
    {% if src_input_mode == 'tbl' %}
        {% set source_tbl = adapter.get_relation(
        database=target.database,
        schema=target.schema,
        identifier=source_table
        ) %}
    {% endif %}

    {% if tgt_input_mode == 'tbl' %}
        {% set target_tbl = adapter.get_relation(
        database=target.database,
        schema=target.schema,
        identifier=target_table
        ) %}
    {% endif %}
    #}

    {% set source_tbl = source_table %}
    {% set target_tbl = target_table %}

    {% if not (source_tbl and target_tbl ) %}
        {% do log("source or target table not found. Skipping the test ")%}
        select null where false
    {% else %}
        {% set filters = " where 1=1 and " %}
        {% if date_column and start_date %}
            {% set src_date_filter = get_date_filter(date_column, start_date, end_date ) %}
        {% endif %}

        {% if target_date_column and start_date %}
            {% set tgt_date_filter = get_date_filter(target_date_column, start_date, end_date) %}
        {% elif date_column and start_date %}
            {% set tgt_date_filter = get_date_filter(date_column, start_date, end_date) %}
        {% endif %}

        {# setting up final filter condition #}
        {% set src_filters = filters ~ src_date_filter | join(" and ") %}
        {% set tgt_filters = filters ~ tgt_date_filter | join(" and ") %}

        {# setting up group and measure columns #}
        {% set src_groups = group_columns.split(',') | map('trim') |list %}
        {% set tgt_groups = tgt_group_columns.split(',') | map('trim') |list %}
        {% set src_measures = measure_columns.split(',') | map('trim') |list %}
        {% set tgt_measures = tgt_measure_columns.split(',') | map('trim') |list %}

        {% if src_input_mode == 'tbl' and tgt_input_mode == 'tbl' %}
            with src as (
            select
                {% for g in src_groups %}
                    {{ g }} as group_col_{{loop.index}}{% if not loop.last %},{% endif %}
                {% endfor %},
                {% for m in src_measures %}
                    sum({{ m }}) as measure_col_{{ loop.index }}{% if not loop.last %},{% endif %}
                {% endfor %}
            from {{ source_tbl }}
            {{ src_filters }}
            group by 
                {% for g in src_groups %}
                    {{ g }}{% if not loop.last %},{% endif %}
                {% endfor %}
            ),
            tgt as (
                select 
                    {% for g in tgt_groups %}
                        {{g}} as group_col_{{loop.index}}{% if not loop.last%},{% endif %}
                    {% endfor %},
                    {% for m in tgt_measures%}
                        sum({{m}}) as measure_col_{{loop.index}}{% if not loop.last%},{% endif %}
                    {% endfor %}
                from {{target_tbl}}
                    {{tgt_filters}}
                group by 
                    {% for g in tgt_groups %}
                        {{g}}{% if not loop.last%},{% endif %}
                    {% endfor %}
            )
            select 
                {% for i in range(1, src_groups|length + 1) %}
                    coalesce(src.group_col_{{ i }}, tgt.group_col_{{ i }}) as group_col_{{ i }},
                {% endfor %}
                {% for i in range(1, src_measures|length + 1) %}
                    src.measure_col_{{ i }} as source_measure_{{ i }},
                    tgt.measure_col_{{ i }} as target_measure_{{ i }}{% if not loop.last %},{% endif %}
                {% endfor %}
            from src full outer join tgt 
                on 
                {% for i in range(1, src_groups |length + 1) %}
                    src.group_col_{{ i }} = tgt.group_col_{{ i }}{% if not loop.last %} and{% endif %}
                {% endfor %}
            where 
                {% for i in range(1, src_measures|length + 1) %}
                    coalesce(src.measure_col_{{ i }}, 0) != coalesce(tgt.measure_col_{{ i }}, 0)
                    {% if not loop.last %} or{% endif %}
                {% endfor %}
            {% do log("Test successfully completed") %}                  
        {% endif %}
    {% endif %}
{% endtest %}