{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- set target_name = target.name -%}

    {%- if custom_schema_name is none -%}

        {# Handle specific cases based on folder names #}
        {% if 'elementary' in node.fqn %}
            {{ target.schema }}_elementary

        {% elif 'staging' in node.fqn and node.fqn.index('staging') + 1 < node.fqn | length %}
            {# Get the subfolder after 'staging' #}
            {% set subfolder = node.fqn[node.fqn.index('staging') + 1] %}
            {% if target_name == 'prod' %}
                prod_staging_{{ subfolder | trim }}
            {% else %}
                dev_staging_{{ subfolder | trim }}
            {% endif %}

        {% elif 'intermediate' in node.fqn and node.fqn.index('intermediate') + 1 < node.fqn | length %}
            {# Get the subfolder after 'intermediate' #}
            {% set subfolder = node.fqn[node.fqn.index('intermediate') + 1] %}
            {% if target_name == 'prod' %}
                prod_intermediate_{{ subfolder | trim }}
            {% else %}
                dev_intermediate_{{ subfolder | trim }}
            {% endif %}

        {% elif 'prod' in node.fqn and node.fqn.index('prod') + 1 < node.fqn | length %}
            {# Get the subfolder after 'prod' #}
            {% set subfolder = node.fqn[node.fqn.index('prod') + 1] %}
            {% if target_name == 'prod' %}
                prod_{{ subfolder | trim }}
            {% else %}
                dev_{{ subfolder | trim }}
            {% endif %}

        {# Fallback to default schema if no specific case matches #}
        {% else %}
            {{ default_schema }}
        {% endif %}

    {%- else -%}

        {{ default_schema }}_{{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}