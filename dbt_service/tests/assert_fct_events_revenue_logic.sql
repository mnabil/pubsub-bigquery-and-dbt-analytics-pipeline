-- Test: Revenue should only exist for purchase events
-- Tests fail when they return rows, so this will fail if non-purchase events have revenue

select 
    source_system,
    event_type,
    count(*) as invalid_records
from {{ ref('fct_events') }}
where revenue > 0 
  and is_purchase = 0
group by source_system, event_type
having count(*) > 0