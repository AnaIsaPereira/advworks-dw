{% snapshot sp_purchaseorderheader %}

{{
    config(
        target_schema='snapshots',
        unique_key='purchase_order_id',
        strategy='timestamp',
        updated_at='last_update'
    )
}}

select
    *
from {{ ref('stg_purchaseorderheader') }}

{% endsnapshot %}
