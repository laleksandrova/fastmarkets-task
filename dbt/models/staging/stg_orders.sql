-- One row per order. Casts raw VARCHAR columns to proper types.
-- Direct casts (not TRY_TO_*) are used deliberately: data profiling
-- found no invalid dates or non-numeric totals in this source, so a
-- defensive TRY_TO_ cast would be complexity without a real problem to
-- solve here - see README.

select
    order_id,
    customer_id,
    order_date::date as order_date,
    order_total::number(10, 2) as order_total
from {{ source('raw', 'raw_orders') }}
