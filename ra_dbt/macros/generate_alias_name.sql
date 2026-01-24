{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
    {%- if custom_alias_name is none -%}
        {%- if node.resource_type == 'model' -%}
            {%- set model_name = node.name -%}
            
            {#- Strip prefixes from staging models -#}
            {%- if node.path.startswith('staging/') -%}
                {{ model_name | replace('stg_', '') }}
            
            {#- Strip prefixes from intermediate models -#}
            {%- elif node.path.startswith('intermediate/') -%}
                {{ model_name | replace('int_', '') }}
            
            {#- Strip prefixes from marts models -#}
            {%- elif node.path.startswith('marts/') -%}
                {{ model_name | replace('run_', '') }}
            
            {#- Default: use model name as-is -#}
            {%- else -%}
                {{ model_name }}
            {%- endif -%}
        {%- else -%}
            {{ node.name }}
        {%- endif -%}
    {%- else -%}
        {{ custom_alias_name | trim }}
    {%- endif -%}
{%- endmacro %}
