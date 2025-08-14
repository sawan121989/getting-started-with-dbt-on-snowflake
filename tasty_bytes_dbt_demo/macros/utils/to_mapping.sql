{% macro to_mapping(input) %}
        {% if input is none %}
            {% do return({}) %}
        {% elif input is string %}
            {% do return({input: input}) %}
        {% elif input is sequence %}
            {% set result = {} %}
            {% for c in input %}
                {% do result.update({c: c}) %}
            {% endfor %}
            {% do log(result) %}
            {% do return(result) %}
        {% elif input is mapping %}
            {% do return(input) %}
        {% else %}
            {% do return(fromjson(input)) %}
        {% endif %}
{% endmacro %}