{% test validate_src_tgt_aggregation_newzzzzz(
    model,
    test_id,
    start_dt=None,
    end_dt=None
) %}

    {% set database_name = 'TASTY_BYTES_DBT_DB' if target.name=='sandbox' else 'TASTY_BYTES_DBT_DB' %}
    {% set schema_name = 'DEV' if target.name=='sandbox' else 'DEV' %}
    {% set qry = 
        "select SOURCE_TABLE,
            TARGET_TABLE,
            GROUP_COLUMNS,
            TGT_GROUP_COLUMNS,
            MEASURE_COLUMNS,
            TGT_MEASURE_COLUMNS,
            DATE_COLUMN,
            TGT_DATE_COLUMN,
            THRESHOLD,
            ACTIVE_IND
         from " ~ database_name ~ "." ~ schema_name ~ ".test_configs
         where test_id = " ~ test_id ~ " and active_ind = 'Y'"
    %}

    {% do log(qry, info=True) %}
    {% set results = run_query(qry) %}

    {% if execute and results|length > 0 %}
        {% set row = results.rows[0] %}
        {% set config = {
            "src_tbl": row[0].values()[0],
            "tgt_tbl": row[1].values()[0],
            "group_columns": fromjson(row[2].values()[0]|string),
            "tgt_group_columns": fromjson(row[3].values()[0]|string),
            "measures_columns": fromjson(row[4].values()[0]|string),
            "tgt_measures_columns": fromjson(row[5].values()[0]|string),
            "date_column": row[6].values()[0],
            "tgt_date_column": row[7].values()[0],
            "threshold": row[8].values()[0],
        } %}
        {% do log (config, info=True) %}

    {% endif %}

    {#% set cfg = get_test_configs(test_id) %#}
    {% set cfg = config %}

    {#
        setting variables differently now
    #}

    {% do log(cfg, info = True) %}
    {# setting up config#}
    {% set source_table = cfg["src_tbl"] %}
    {% set target_table = cfg['tgt_tbl'] %}
    {% set group_columns = cfg['group_columns'] %}
    {% set tgt_group_columns = cfg['tgt_group_columns'] %}
    {% set measures_columns = cfg['measures_columns'] %}
    {% set tgt_measures_columns = cfg['tgt_measures_columns'] %}
    {% set threshold = cfg['threshold'] %}
    {% set date_column = cfg['date_column'] %}
    {% set tgt_date_column = cfg['tgt_date_column'] %}
    {% set threshold = cfg['threshold'] %}

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
            ),
            diff as (
            select
              {% for g in src_groups %}
                    coalesce(s.group_col_{{loop.index}}, t.group_col_{{loop.index}}) as group_col_{{loop.index}},
                    {% endfor %}
                    {% for m in src_measures %}
                    coalesce(s.measure_col_{{loop.index}}, t.measure_col_{{loop.index}}) as measure_col_{{loop.index}},
                    {% endfor %}
              {% for m in src_measures %}
                abs(coalesce(s.measure_col_{{ loop.index }},0) - coalesce(t.measure_col_{{ loop.index }},0)) as diff_{{ m }},
                case
                  when abs(coalesce(s.measure_col_{{ loop.index }},0) - coalesce(t.measure_col_{{ loop.index }},0))
                    <= coalesce({{ threshold | default(0) }},0)
                      * nullif(abs(coalesce(t.measure_col_{{ loop.index }},0)),0)
                  then 0 else 1 end as fail_flag_{{ loop.index }}
                {% if not loop.last %},{% endif %}
              {% endfor %}
            from src s
            full outer join tgt t
            on {% for g in src_groups %}
              s.group_col_{{loop.index}} = t.group_col_{{loop.index}}
              {% if not loop.last %} and {% endif %}
             {% endfor %}
           )
            select *
          from diff
          where
            {% for g in src_groups %}
             fail_flag_{{ loop.index }} = 1
             {% if not loop.last %} or {% endif %}
            {% endfor %}
            {% do log("Test successfully completed") %}                  
        {% endif %}
    {% endif %}
{% endtest %}