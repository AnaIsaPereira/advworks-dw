select
    shipmethodid as ship_method_id,
    name as ship_method_name,
    shipbase as ship_base,
    shiprate as ship_rate,
    modifieddate as last_update
from {{ source("purchasing", "shipmethod") }}
