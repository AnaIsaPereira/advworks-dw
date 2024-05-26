with vendor as (
    select
        businessentityid as vendor_id,
        name as vendor_name,
        modifieddate as vendor_last_update
    from {{ source('purchasing', 'vendor') }}
),
vendoraddress as (
    select *
    from {{ ref('stg_vendoraddress') }}
),
vendorcontact as (
    select *
    from {{ ref('stg_vendorcontact') }}
)

select
    v.vendor_id,
    v.vendor_name,
    v.vendor_last_update,
    va.address_line_1,
    va.address_line_2,
    va.city,
    va.postal_code,
    va.province_name,
    va.country_region_code,
    va.address_last_update,
    vc.email,
    vc.phone_number,
    vc.contact_last_update,
    GREATEST(v.vendor_last_update, va.address_last_update, vc.contact_last_update) as last_update
from vendor v
left join vendoraddress va on v.vendor_id = va.vendor_id
left join vendorcontact vc on v.vendor_id = vc.vendor_id