{{ config(materialized='table') }}

-- Simple date dimension for time-based analysis
with date_spine as (
  select distinct event_date as date_day
  from {{ ref('fct_events') }}
  where event_date is not null
)

select
  date_day,
  
  -- Date parts
  extract(year from date_day) as year_number,
  extract(month from date_day) as month_number, 
  extract(day from date_day) as day_of_month,
  extract(dayofweek from date_day) as day_of_week_number,
  
  -- Formatted dates
  format_date('%B', date_day) as month_name,
  format_date('%A', date_day) as day_name,
  format_date('%Y-%m', date_day) as year_month,
  
  -- Business calendar
  case when extract(dayofweek from date_day) in (1, 7) then true else false end as is_weekend

from date_spine
order by date_day