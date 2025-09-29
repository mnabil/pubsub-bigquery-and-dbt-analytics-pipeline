{{ config(
    materialized='table',
    cluster_by=['user_id'],
    labels={'layer': 'marts', 'grain': 'user'}
) }}

select
  user_id,
  current_date() as analysis_date,
  
  -- Basic browsing metrics
  count(distinct session_id) as total_sessions,
  count(*) as total_page_views,
  count(distinct event_date) as browsing_days,
  count(distinct category) as categories_browsed,
  
  -- Top category
  array_agg(category ignore nulls order by category limit 1)[safe_offset(0)] as top_category,
  
  -- Engagement
  sum(case when event_type like '%cart%' then 1 else 0 end) as cart_adds,
  round(avg(case when session_id is not null then 1 else 0 end) * 100, 1) as session_rate_pct,
  
  -- Recency
  date_diff(current_date(), max(event_date), day) as days_since_last_browse,
  
  -- Simple segmentation
  case 
    when count(*) >= 50 then 'Heavy Browser'
    when count(*) >= 10 then 'Regular Browser' 
    else 'Light Browser'
  end as browsing_segment

from {{ ref('fct_events') }}
where (is_page_view = 1 or is_cart_add = 1)
  and user_id is not null
group by user_id