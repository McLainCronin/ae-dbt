with source as (
    select * from {{ source('northwind', 'order_status') }}
)

select * from source
