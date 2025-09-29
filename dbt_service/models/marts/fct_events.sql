{{ config(
    materialized='table',
    partition_by={
      "field": "event_date", 
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=['event_type', 'user_id'],
    labels={'layer': 'marts', 'grain': 'event'}
) }}

-- Central fact table combining clickstream and transaction events
with clickstream_events as (
  select
    'clickstream' as source_system,
    event_id,
    user_id,
    session_id,
    event_type,
    event_timestamp,
    date(event_timestamp) as event_date,
    device_type,
    country,
    
    -- Product context (flattened from properties)
    product_id,
    category,
    price,
    quantity,
    
    -- Measures
    case when event_type = 'purchase' then price * quantity else 0 end as revenue,
    case when event_type = 'purchase' then quantity else 0 end as quantity_sold,
    
    -- Event flags  
    case when event_type = 'purchase' then 1 else 0 end as is_purchase,
    case when event_type = 'page_view' then 1 else 0 end as is_page_view,
    case when event_type = 'add_to_cart' then 1 else 0 end as is_cart_add

  from {{ ref('stg_clickstream') }}
),

transaction_events as (
  select
    'transactions' as source_system,
    transaction_id as event_id,
    user_id,
    cast(null as string) as session_id,  -- transactions don't have sessions
    'transaction_' || status as event_type,  -- transaction_completed, transaction_failed
    timestamp as event_timestamp,
    date(timestamp) as event_date,
    cast(null as string) as device_type,  -- not in transaction data
    billing_country as country,
    
    -- Product context (would need to parse items JSON - keeping simple for now)
    cast(null as string) as product_id,
    cast(null as string) as category, 
    cast(null as float64) as price,
    cast(null as int64) as quantity,
    
    -- Measures
    case when status = 'completed' then amount else 0.0 end as revenue,
    cast(0 as int64) as quantity_sold,  -- would parse from items
    
    -- Event flags
    case when status = 'completed' then 1 else 0 end as is_purchase,
    0 as is_page_view,
    0 as is_cart_add

  from {{ ref('stg_transactions') }}
)

-- Union all events into single fact table
select * from clickstream_events
union all
select * from transaction_events