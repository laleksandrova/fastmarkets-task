-- Flattens the order_items JSON array (held as VARIANT in raw) into one
-- row per product per order. Grain: (order_id, product_id) - confirmed
-- during data profiling that no order repeats the same product, so this
-- pair is a safe natural key without needing a surrogate key.

with source as (
    select
        order_id,
        order_items
    from {{ source('raw', 'raw_orders') }}
),

flattened as (
    select
        source.order_id,
        item.value:product_id::varchar as product_id,
        item.value:quantity::number as quantity,
        item.value:price::number(10, 2) as price
    from source,
    lateral flatten(input => source.order_items) as item
)

select
    order_id,
    product_id,
    quantity,
    price,
    quantity * price as line_total
from flattened
