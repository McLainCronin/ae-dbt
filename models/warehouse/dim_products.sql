{{
    config(
        materialized='table',
        schema='dwh_northwind'
    )
}}

with source as (
    select
        p.ID as product_id,
        p.product_code,
        p.product_name,
        p.description,
        s.company as supplier_company,
        p.standard_cost,
        p.list_price,
        p.reorder_level,
        p.target_level,
        p.quantity_per_unit,
        p.discontinued,
        p.minimum_reorder_quantity,
        p.category,
        p.attachments,
        current_timestamp() AS insertion_timestamp
    from {{ ref('stg_products')}} p
    left join {{ ref('stg_suppliers')}} s
        on s.company = company  -- Removed p. prefix
),

unique_source as (
    select *,
        row_number() over (partition by product_id order by product_id) as row_number
    from source
)

select 
    product_id,
    product_code,
    product_name,
    description,
    supplier_company,
    standard_cost,
    list_price,
    reorder_level,
    target_level,
    quantity_per_unit,
    discontinued,
    minimum_reorder_quantity,
    category,
    attachments,
    insertion_timestamp
from unique_source
where row_number = 1