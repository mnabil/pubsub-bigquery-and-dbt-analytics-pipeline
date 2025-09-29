{{ config(
    materialized='table',
    cluster_by=['user_id'],
    labels={'layer': 'marts', 'grain': 'user'}
) }}

select
  user_id,
  current_date() as analysis_date,
  
  -- Basic purchase metrics
  count(*) as total_purchases,
  sum(revenue) as total_revenue,
  round(avg(revenue), 2) as avg_order_value,
  
  -- Recency
  date_diff(current_date(), max(event_date), day) as days_since_last_purchase,
  
  -- Simple RFM segmentation
  case 
    when date_diff(current_date(), max(event_date), day) <= 30 
         and count(*) >= 3 
         and sum(revenue) >= 200 then 'High Value'
    when date_diff(current_date(), max(event_date), day) <= 60 
         and count(*) >= 2 then 'Active'
    when date_diff(current_date(), max(event_date), day) > 90 then 'At Risk'
    else 'Regular'
  end as customer_segment

from {{ ref('fct_events') }}
where is_purchase = 1 
  and revenue > 0 
  and user_id is not null
group by user_id