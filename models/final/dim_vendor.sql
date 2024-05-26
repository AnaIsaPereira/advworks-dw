with
    first_version as (
        select
            dbt_scd_id,
            vendor_id,
            vendor_name,
            address_line_1,
            address_line_2,
            city,
            postal_code,
            province_name,
            country_region_code,
            email,
            phone_number,
            dbt_valid_from,
            dbt_valid_to,
            dbt_updated_at,
            row_number() over (partition by vendor_id order by dbt_valid_from) as row_nr
        from {{ ref("sp_vendor") }}
    )
select
    dbt_scd_id as sk_vendor,
    vendor_id,
    vendor_name,
    address_line_1,
    address_line_2,
    city,
    postal_code,
    province_name,
    country_region_code,
    email,
    phone_number,
    case when row_nr = 1 then '1970-01-01' else dbt_valid_from end as valid_from,
    coalesce(dbt_valid_to, '2200-01-01') as valid_to,
    dbt_updated_at as last_updated_at
from first_version
