{% snapshot sp_product %}

{{
    config(
        target_schema='snapshots',
        unique_key='product_id',
        strategy='timestamp',
        updated_at='last_update'
    )
}}

select
    *
from {{ ref('stg_product') }}

{% endsnapshot %}
