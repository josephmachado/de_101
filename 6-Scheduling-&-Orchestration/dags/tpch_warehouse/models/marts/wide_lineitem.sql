with

fct_lineitem as (
    select * from {{ ref('fct_lineitems') }}
),

wide_lineitem as (
    select * from fct_lineitem
)

select * from wide_lineitem