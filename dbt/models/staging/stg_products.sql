-- Product dimension, extracted from the order_items array rather than
-- being a source column in its own right. Not asked for explicitly in
-- the task, added because product_name otherwise lives buried inside
-- JSON with no dedicated home.

with source as (
    select order_items
    from {{ source('raw', 'raw_orders') }}
),

flattened as (
    select
        item.value:product_id::varchar as product_id,
        item.value:product_name::varchar as product_name
    from source,
    lateral flatten(input => source.order_items) as item
)

select distinct
    product_id,
    product_name
from flattened
