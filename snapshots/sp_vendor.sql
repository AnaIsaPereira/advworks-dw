{% snapshot sp_vendor %}

{{
    config(
        target_schema='snapshots',
        unique_key='vendor_id',
        strategy='timestamp',
        updated_at='last_update'
    )
}}

select
    *
from {{ ref('stg_vendor') }}

{% endsnapshot %}
