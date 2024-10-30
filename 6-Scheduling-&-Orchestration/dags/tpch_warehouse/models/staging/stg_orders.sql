with

source as (

    select * from {{ source('raw_layer', 'orders') }}

),

renamed as (

    select

        ---------- ids
        o_orderkey as orderkey,

        ---------- references
        o_custkey as custkey,

        ---------- status and pricing
        o_orderstatus as orderstatus,
        o_totalprice as totalprice,

        ---------- dates
        o_orderdate as orderdate,

        ---------- priority and clerk
        o_orderpriority as orderpriority,
        o_clerk as clerk,
        o_shippriority as shippriority,

        ---------- comments
        o_comment as comment

    from source

)

select * from renamed
