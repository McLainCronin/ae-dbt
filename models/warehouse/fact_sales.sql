{{ config(
    partition_by={
        "field": "order_date",
        "data_type": "date",
    }
) }}

with source as (
   select
       od.id as order_detail_id,
       od.order_id,
       od.product_id,
       o.customer_id,
       o.employee_id,
       o.shipper_id,
       od.quantity,
       od.unit_price,
       od.discount,
       od.status_id as order_detail_status_id,
       od.date_allocated,
       od.purchase_order_id,
       od.inventory_id,
       date(o.order_date) as order_date,
       o.shipped_date,
       o.paid_date,
       o.shipping_fee,
       o.taxes,
       o.payment_type,
       o.tax_rate,
       o.tax_status_id,
       o.status_id as order_status_id,
       current_timestamp() as insertion_timestamp
   from {{ ref('stg_order_details')}} od
   left join {{ ref('stg_orders')}} o
       on od.order_id = o.id
   where od.order_id is not null
),

unique_source as (
   select *,
       row_number() over (
           partition by 
               customer_id, 
               employee_id, 
               order_id, 
               product_id, 
               shipper_id, 
               purchase_order_id,
               order_date
           order by order_id
       ) as row_number
   from source
)

select 
   order_detail_id,
   order_id,
   product_id,
   customer_id,
   employee_id,
   shipper_id,
   quantity,
   unit_price,
   discount,
   order_detail_status_id,
   date_allocated,
   purchase_order_id,
   inventory_id,
   order_date,
   shipped_date,
   paid_date,
   shipping_fee,
   taxes,
   payment_type,
   tax_rate,
   tax_status_id,
   order_status_id,
   insertion_timestamp
from unique_source
where row_number = 1