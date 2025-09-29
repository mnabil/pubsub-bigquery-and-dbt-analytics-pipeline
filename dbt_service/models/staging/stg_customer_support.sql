{{ config(materialized='view') }}

select
    -- Pub/Sub metadata for data lineage and debugging
    subscription_name,
    message_id,
    publish_time,
    attributes as pubsub_attributes,
    
    -- Business support data
    ticket_id,
    user_id,
    cast(created_at as timestamp) as created_at,
    category,
    priority,
    status,
    subject,
    description

from {{ source('raw', 'customer_support_tickets') }}