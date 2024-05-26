select
    purchaseorderid as purchase_order_id,
    revisionnumber as revision_number,
    status,
    employeeid as employee_id,
    vendorid as vendor_id,
    shipmethodid as ship_method_id,
    orderdate as order_date,
    shipdate as ship_date,
    subtotal as sub_total,
    taxamt as taxa_mt,
    freight as ship_cost,
    modifieddate as last_update
from {{ source("purchasing", "purchaseorderheader") }}