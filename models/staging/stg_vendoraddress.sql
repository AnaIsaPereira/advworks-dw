with ranked_vendoraddress as (
    select
        g.businessentityid as vendor_id,
        h.addressline1 as address_line_1,
        h.addressline2 as address_line_2,
        h.city,
        h.postalcode as postal_code,
        i.name as province_name,
        i.countryregioncode as country_region_code,
        GREATEST(h.modifieddate, i.modifieddate) as address_last_update,
        ROW_NUMBER() OVER (PARTITION BY g.businessentityid ORDER BY GREATEST(h.modifieddate, i.modifieddate) DESC) AS row_num
    from {{ source("person", "businessentityaddress") }} g
    left join {{ source("person", "address") }} h on g.addressid = h.addressid
    left join {{ source("person", "stateprovince") }} i on h.stateprovinceid = i.stateprovinceid
      )
select
    vendor_id,
    address_line_1,
    address_line_2,
    city,
    postal_code,
    province_name,
    country_region_code,
    address_last_update
FROM
    ranked_vendoraddress
WHERE
    row_num = 1
