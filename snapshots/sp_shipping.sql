{% snapshot sp_shipping %}

{{
    config(
        target_schema='snapshots',
        unique_key='ship_method_id',
        strategy='timestamp',
        updated_at='last_update'
    )
}}

select
    *
from {{ ref('stg_shipping') }}

{% endsnapshot %}
