with
    first_version as (
        select
            dbt_scd_id,
            ship_method_id,
            ship_method_name,
            ship_base,
            ship_rate,
            dbt_valid_from,
            dbt_valid_to,
            dbt_updated_at,
            row_number() over (partition by ship_method_id order by dbt_valid_from) as row_nr
        from {{ ref("sp_shipping") }}
    )
select
    dbt_scd_id as sk_shipping,
    ship_method_id,
    ship_method_name,
    ship_base,
    ship_rate,
    case when row_nr = 1 then '1970-01-01' else dbt_valid_from end as valid_from,
    coalesce(dbt_valid_to, '2200-01-01') as valid_to,
    dbt_updated_at as last_updated_at
from first_version
