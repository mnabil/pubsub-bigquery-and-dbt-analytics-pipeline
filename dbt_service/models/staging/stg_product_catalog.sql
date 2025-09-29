{{ config(materialized='view') }}

select
    product_id,
    name as product_name,
    category,
    subcategory,
    cast(price as float64) as price,
    cast(cost as float64) as cost,
    cast(stock_quantity as int64) as stock_quantity,
    supplier_id

from {{ source('raw', 'product_catalog') }}