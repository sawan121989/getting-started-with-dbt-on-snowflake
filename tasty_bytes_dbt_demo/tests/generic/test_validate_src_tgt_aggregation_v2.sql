{% test validate_src_tgt_aggregation_v2(
    model,
    test_id,
    start_dt=None,
    end_dt=None
) %}
    {{ mac_validate_src_tgt(test_id, start_dt, end_dt)}}
{% endtest %}