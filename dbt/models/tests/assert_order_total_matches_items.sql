-- Singular dbt test. Returns rows for any order where order.order_total
-- doesn't match the sum of quantity*price across its line items - dbt
-- treats a non-empty result set as a test failure.
--
-- This checks the model, not just the schema: it fails if a future dbt
-- change (e.g. a bad join, a bug in stg_order_items) breaks the
-- relationship between the two tables, even though each table's own
-- columns would still pass their individual not_null/unique tests.

select
    o.order_id,
    o.order_total,
    sum(oi.line_total) as computed_total
from {{ ref('order') }} o
join {{ ref('order_item') }} oi on o.order_id = oi.order_id
group by o.order_id, o.order_total
having abs(o.order_total - sum(oi.line_total)) > 0.01
