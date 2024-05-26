select
    date_key as sk_date,
    date,
    day_of_month,
    month_name,
    year,
    day_name,
    is_holiday,
    quarter
from {{ ref("stg_dim_date") }}
