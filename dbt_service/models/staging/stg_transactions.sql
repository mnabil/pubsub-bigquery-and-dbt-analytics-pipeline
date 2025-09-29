{{ config(materialized='view') }}

select
    -- Pub/Sub metadata for data lineage and debugging
    subscription_name,
    message_id,
    publish_time,
    attributes as pubsub_attributes,
    
    -- Business transaction data
    transaction_id,
    user_id,
    cast(timestamp as timestamp) as timestamp,
    cast(amount as float64) as amount,
    currency,
    payment_method,
    status,
    items,
    shipping_address,
    billing_country

from {{ source('raw', 'transaction_events') }}