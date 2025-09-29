{{ config(
    materialized='table',
    cluster_by=['user_segment'],
    labels={'layer': 'marts', 'grain': 'user'}
) }}

select
  coalesce(p.user_id, b.user_id) as user_id,
  current_date() as analysis_date,
  
  -- Purchase metrics
  coalesce(p.total_purchases, 0) as total_purchases,
  coalesce(p.total_revenue, 0) as total_revenue,
  p.avg_order_value,
  p.days_since_last_purchase,
  p.customer_segment as purchase_segment,
  
  -- Browse metrics  
  coalesce(b.total_page_views, 0) as total_page_views,
  coalesce(b.total_sessions, 0) as total_sessions,
  b.top_category,
  coalesce(b.cart_adds, 0) as cart_adds,
  b.days_since_last_browse,
  b.browsing_segment,
  
  -- Combined user segmentation
  case
    when p.customer_segment = 'High Value' then 'VIP Customer'
    when p.total_purchases > 0 and b.total_page_views >= 20 then 'Engaged Buyer'
    when p.total_purchases > 0 then 'Buyer'
    when b.cart_adds > 0 then 'Intent Browser'
    when b.total_page_views >= 10 then 'Active Browser'
    when b.total_page_views > 0 then 'Casual Browser'
    else 'New Visitor'
  end as user_segment,
  
  -- Conversion rate
  case 
    when b.total_page_views > 0 
    then round(safe_divide(p.total_purchases, b.total_page_views) * 100, 2)
    else 0 
  end as conversion_rate_pct

from {{ ref('fct_user_purchases') }} p
full outer join {{ ref('fct_user_browsing') }} b using(user_id)