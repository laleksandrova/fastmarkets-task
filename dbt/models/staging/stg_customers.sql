-- One row per customer. Cleans up two real data quality issues found in
-- the source: phone numbers with literal embedded quote characters, and
-- known placeholder values in phone/email (blank, "123456", "N/A",
-- "invalid-email") that should be null rather than kept as fake strings.

with source as (
    select * from {{ source('raw', 'raw_orders') }}
),

renamed as (
    select
        customer_id,
        customer_name,
        nullif(trim(replace(customer_phone, '"', '')), '') as customer_phone,
        nullif(lower(trim(customer_email)), '') as customer_email
    from source
)

select
    customer_id,
    customer_name,
    case
        when customer_phone in ('123456', 'N/A') then null
        else customer_phone
    end as customer_phone,
    case
        when customer_email in ('n/a', 'invalid-email') then null
        else customer_email
    end as customer_email
from renamed
