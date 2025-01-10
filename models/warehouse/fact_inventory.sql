{{ config(
    partition_by={
        "field": "transaction_created_date",
        "data_type": "date",
    }
) }}
WITH source AS (
    SELECT
        id AS inventory_id,
        transaction_type,
        date(transaction_created_date) as transaction_created_date,
        transaction_modified_date,
        product_id,
        quantity,
        purchase_order_id,
        customer_order_id,
        comments,
        CURRENT_TIMESTAMP() AS insertion_timestamp
    FROM {{ ref('stg_inventory_transactions') }}
),
unique_source AS (
    SELECT
        inventory_id,
        transaction_type,
        transaction_created_date,
        transaction_modified_date,
        product_id,
        quantity,
        purchase_order_id,
        customer_order_id,
        comments,
        insertion_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY inventory_id
            ORDER BY inventory_id
        ) AS row_number
    FROM source
)

SELECT
    inventory_id,
    transaction_type,
    transaction_created_date,
    transaction_modified_date,
    product_id,
    quantity,
    purchase_order_id,
    customer_order_id,
    comments,
    insertion_timestamp
FROM unique_source
WHERE row_number = 1