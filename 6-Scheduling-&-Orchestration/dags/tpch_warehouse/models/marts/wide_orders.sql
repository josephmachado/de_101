with

fct_orders as (
    select * from {{ ref('fct_orders') }}
),

dim_customer as (
    select * from {{ ref('dim_customers') }}
),

wide_orders as (
    select 
        o.*,
        c.*
    from fct_orders as o
    left join dim_customer as c 
    on o.custkey = c.customer_key
)

select * from wide_orders