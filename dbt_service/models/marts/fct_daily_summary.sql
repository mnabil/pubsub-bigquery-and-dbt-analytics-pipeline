{{ config(
    materialized='table',
    partition_by={
      "field": "event_date",
      "data_type": "date", 
      "granularity": "day"
    },
    cluster_by=['country'],
    labels={'layer': 'marts', 'grain': 'daily'}
) }}

-- Daily summary fact table for reporting
select
  event_date,
  country,
  
  -- Volume metrics
  count(*) as total_events,
  count(distinct user_id) as daily_active_users,
  count(distinct session_id) as daily_sessions,
  
  -- Event breakdown
  sum(is_page_view) as page_views,
  sum(is_cart_add) as cart_adds, 
  sum(is_purchase) as purchases,
  
  -- Revenue metrics
  sum(revenue) as daily_revenue,
  count(distinct case when revenue > 0 then user_id end) as daily_buyers,
  
  -- Conversion rates
  round(safe_divide(sum(is_cart_add) * 100.0, sum(is_page_view)), 2) as view_to_cart_rate,
  round(safe_divide(sum(is_purchase) * 100.0, sum(is_cart_add)), 2) as cart_to_purchase_rate,
  
  -- Average metrics
  round(safe_divide(sum(revenue), sum(is_purchase)), 2) as avg_order_value

from {{ ref('fct_events') }}
where event_date is not null
group by event_date, country