{{ config(
    materialized='table',
    indexes=[{'columns': ['user_id'], 'unique': true}]
) }}

-- User dimension with attributes from both clickstream and transactions
with user_metrics as (
  select
    user_id,
    
    -- Activity timeline
    min(event_date) as first_activity_date,
    max(event_date) as last_activity_date,
    count(distinct event_date) as active_days,
    
    -- Geography (most common country)  
    approx_top_count(country, 1)[offset(0)].value as primary_country,
    
    -- Engagement
    count(*) as total_events,
    sum(is_page_view) as page_views,
    sum(is_cart_add) as cart_adds,
    sum(is_purchase) as purchases,
    
    -- Revenue
    sum(revenue) as lifetime_value,
    
    -- Customer classification
    case 
      when sum(is_purchase) >= 3 then 'High Value Customer'
      when sum(is_purchase) >= 1 then 'Customer'
      when sum(is_cart_add) >= 1 then 'Engaged Prospect'
      else 'Browser'
    end as customer_segment

  from {{ ref('fct_events') }}
  where user_id is not null
  group by user_id
)

select
  user_id,
  first_activity_date,
  last_activity_date, 
  active_days,
  primary_country,
  total_events,
  page_views,
  cart_adds,
  purchases,
  round(cast(lifetime_value as numeric), 2) as lifetime_value,
  customer_segment,
  
  -- Recency
  date_diff(current_date(), last_activity_date, day) as days_since_last_activity

from user_metrics
order by lifetime_value desc