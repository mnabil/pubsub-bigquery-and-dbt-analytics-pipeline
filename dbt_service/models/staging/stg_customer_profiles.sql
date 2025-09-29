{{ config(
    materialized='view'
) }}

select
  user_id as customer_id,
  current_date() as profile_date,
  
  -- Customer attributes that can change over time
  user_segment as current_segment,
  purchase_segment,
  browsing_segment,
  
  -- Behavioral attributes
  case 
    when total_revenue >= 1000 then 'High'
    when total_revenue >= 200 then 'Medium'  
    else 'Low'
  end as value_tier,
  
  case
    when days_since_last_purchase <= 30 then 'Active'
    when days_since_last_purchase <= 90 then 'Lapsed'
    else 'Inactive'  
  end as activity_status,
  
  case
    when conversion_rate_pct >= 5 then 'High Converter'
    when conversion_rate_pct >= 1 then 'Medium Converter'
    else 'Low Converter'
  end as conversion_profile,
  
  -- Supporting metrics
  total_purchases,
  total_revenue,
  total_page_views,
  conversion_rate_pct

from {{ ref('dim_user_segments') }}