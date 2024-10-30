with

wide_orders as (
    select * from {{ ref('wide_orders') }}
),

order_lineitem_metrics as (
    select 
        orderkey as order_key,
        count(linenumber) as num_lineitems
    from {{ ref('wide_lineitem') }}
    group by orderkey
),

customer_outreach_metrics as (
    select 
        o.customer_key,
        o.customer_name,
        min(o.totalprice) as min_order_value,
        max(o.totalprice) as max_order_value,
        avg(o.totalprice) as avg_order_value,
        avg(m.num_lineitems) as avg_num_items_per_order
    from wide_orders as o
    left join order_lineitem_metrics as m 
    on o.orderkey = m.order_key
    group by o.customer_key, o.customer_name
)

select * from customer_outreach_metrics