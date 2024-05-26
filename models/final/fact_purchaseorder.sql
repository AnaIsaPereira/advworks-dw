with
    dimension_ids as (
        select
            pod.purchase_order_detail_id,
            pod.purchase_order_id,
            pod.order_quantity,
            pod.product_id,
            pod.unit_price,
            pod.received_quantity,
            pod.rejected_quantity,
            poh.employee_id,
            poh.vendor_id,
            poh.ship_method_id,
            poh.order_date,
            poh.ship_date
        from {{ ref("stg_purchaseorderdetail") }} pod
        left join
            {{ ref("stg_purchaseorderheader") }} poh
            on pod.purchase_order_id = poh.purchase_order_id
    ),

    surrogate_keys as (
        select
            purchase_order_detail_id,
            dpu.purchase_order_id as sk_purchase_order,
            order_dd.sk_date as sk_order_date,
            ship_dd.sk_date as sk_ship_date,
            de.sk_employee as sk_employee,
            dp.sk_product as sk_product,
            ds.sk_shipping as sk_shipping,
            dv.sk_vendor as sk_vendor,
            order_quantity,
            unit_price,
            order_quantity * unit_price as total_line,
            received_quantity,
            rejected_quantity
        from dimension_ids dids
        join {{ ref("dim_date") }} order_dd on dids.order_date = order_dd.date
        left join {{ ref("dim_date") }} ship_dd on dids.ship_date = ship_dd.date
        join
            {{ ref("dim_employee") }} de
            on dids.employee_id = de.employee_id
            and dids.order_date between de.valid_from and de.valid_to
        left join
            {{ ref("dim_product") }} dp
            on dids.product_id = dp.product_id
            and dids.order_date between dp.valid_from and dp.valid_to
        left join
            {{ ref("dim_shipping") }} ds
            on dids.ship_method_id = ds.ship_method_id
            and dids.order_date between ds.valid_from and ds.valid_to
        left join
            {{ ref("dim_vendor") }} dv
            on dids.vendor_id = dv.vendor_id
            and dids.order_date between dv.valid_from and dv.valid_to
        left join
            {{ ref("dim_purchaseorderheader") }} dpu
            on dids.purchase_order_id = dpu.purchase_order_id
            and dids.order_date between dv.valid_from and dv.valid_to
    ),

    final as (
        select
            purchase_order_detail_id,
            sk_purchase_order,
            sk_order_date,
            sk_ship_date,
            sk_employee,
            sk_product,
            sk_shipping,
            sk_vendor,
            order_quantity,
            unit_price,
            total_line,
            received_quantity,
            rejected_quantity
        from surrogate_keys
    )

select *
from final
