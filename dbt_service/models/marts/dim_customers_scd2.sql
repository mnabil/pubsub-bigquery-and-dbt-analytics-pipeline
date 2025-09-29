{{ config(
    materialized='table',
    cluster_by=['customer_id'],
    labels={'layer': 'marts', 'grain': 'customer_history'}
) }}

with current_profiles as (
  select 
    *,
    row_number() over (partition by customer_id order by profile_date desc) as rn
  from {{ ref('stg_customer_profiles') }}
),

-- Get the latest profile for each customer
latest_profiles as (
  select * 
  from current_profiles 
  where rn = 1
),

-- Historical data (this would come from previous runs in production)
-- For now, we'll simulate some historical changes
historical_profiles as (
  select
    customer_id,
    date_sub(profile_date, interval 30 day) as profile_date,
    case 
      when current_segment = 'VIP Customer' then 'Engaged Buyer'
      when current_segment = 'Engaged Buyer' then 'Buyer'  
      else 'Active Browser'
    end as current_segment,
    purchase_segment,
    browsing_segment,
    case
      when value_tier = 'High' then 'Medium'
      else 'Low' 
    end as value_tier,
    'Active' as activity_status,
    'Low Converter' as conversion_profile,
    greatest(total_purchases - 2, 0) as total_purchases,
    greatest(total_revenue - 100, 0) as total_revenue,
    greatest(total_page_views - 10, 0) as total_page_views,
    greatest(conversion_rate_pct - 1, 0) as conversion_rate_pct,
    1 as rn  -- Add the missing column
  from latest_profiles
  where total_purchases > 1  -- Only create history for customers with activity
),

-- Combine current and historical
all_profiles as (
  select * from latest_profiles
  union all
  select * from historical_profiles
),

-- Add SCD Type 2 fields
scd2_profiles as (
  select
    row_number() over (order by customer_id, profile_date) as customer_key,
    customer_id,
    profile_date as effective_date,
    
    -- Lead function to get next profile date for this customer
    lead(profile_date) over (
      partition by customer_id 
      order by profile_date
    ) as expiration_date,
    
    -- Current record flag
    case 
      when lead(profile_date) over (
        partition by customer_id 
        order by profile_date
      ) is null then true 
      else false 
    end as is_current,
    
    -- All the profile attributes
    current_segment,
    purchase_segment,
    browsing_segment,
    value_tier,
    activity_status,
    conversion_profile,
    total_purchases,
    total_revenue,
    total_page_views,
    conversion_rate_pct,
    
    -- Metadata
    current_timestamp() as created_at,
    'dbt_model' as source_system
    
  from all_profiles
)

select * from scd2_profiles