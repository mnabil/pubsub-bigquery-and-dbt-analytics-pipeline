{{ config(
    materialized='table',
    cluster_by=['category'],
    labels={'layer': 'marts', 'grain': 'user_category'}
) }}

select
  user_id,
  category,
  
  -- Simple funnel metrics
  sum(is_page_view) as page_views,
  sum(is_cart_add) as cart_adds, 
  sum(is_purchase) as purchases,
  sum(revenue) as revenue,
  
  -- Simple classification
  case
    when sum(is_purchase) > 0 then 'Converter'
    when sum(is_cart_add) > 0 then 'Cart User'  
    else 'Browser'
  end as journey_stage

from {{ ref('fct_events') }}
where category is not null 
  and user_id is not null
  and is_page_view = 1
group by user_id, category