with

wide_lineitem as (
    select * from {{ ref('wide_lineitem') }}
),

order_lineitem_metrics as (
    select 
        orderkey as order_key,
        count(linenumber) as num_lineitems
    from wide_lineitem
    group by orderkey
)

select * from order_lineitem_metrics