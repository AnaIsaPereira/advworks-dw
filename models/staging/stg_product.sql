select
    a.productid as product_id,
    a.name as product_name,
    a.productnumber as product_number,
    a.modifieddate as product_last_update,
    b.name as subcategory_name,
    b.modifieddate as subcategory_last_update,
    c.name as category_name,
    c.modifieddate as category_last_update,
    GREATEST(a.modifieddate, b.modifieddate, c.modifieddate) as last_update
from {{ source("production", "product") }} a
    left join {{ source("production", "productsubcategory") }} b on a.productsubcategoryid = b.productsubcategoryid
    left join {{ source("production", "productcategory") }} c on b.productcategoryid = c.productcategoryid