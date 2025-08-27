{% macro get_test_configs(test_id) %}

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

    {% set results = run_query(qry) %}

    {% if execute and results|length > 0 %}
        {% set row = results.columns %}
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
        {{ return(config) }}
    {% else %}
        {{ return({}) }}
    {% endif %}
{% endmacro %}
