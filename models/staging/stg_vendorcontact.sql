select
    j.businessentityid as vendor_id,
    j.emailaddress as email,
    j.modifieddate as email_last_update,
    k.phonenumber as phone_number,
    k.modifieddate as phone_number_last_update,
    GREATEST(j.modifieddate, k.modifieddate) as contact_last_update
from {{ source("person", "emailaddress") }} j
    left join {{ source("person", "personphone") }} k on j.businessentityid = k.businessentityid