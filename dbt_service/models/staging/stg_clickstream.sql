{{ config(materialized='view') }}

select
  -- Pub/Sub metadata for data lineage and debugging
  subscription_name,
  message_id,
  publish_time,
  attributes as pubsub_attributes,
  
  -- Business event data
  event_id,
  user_id,
  session_id,
  event_type,
  timestamp as event_timestamp,
  lower(page_url) as page_url,
  user_agent,
  ip_address,
  referrer,
  device_type,
  country,

  -- Flatten nested RECORD
  properties.product_id,
  properties.category,
  properties.price,
  properties.quantity,
  properties.cart_value,
  properties.items_count,
  properties.results_count,
  properties.search_query

from {{ source('raw', 'clickstream_events') }}
where not exists (
  select 1
  from {{ ref('quarantined_events') }} qe
  where qe.event_id = clickstream_events.event_id
)
and not exists (
  select 1
  from {{ ref('quarantined_sessions') }} qs
  where qs.session_id = clickstream_events.session_id
)
