-- Test: Revenue should never be negative
-- Business rule: all revenue values must be >= 0

select *
from {{ ref('fct_events') }}
where revenue < 0