with

source as (

    select * from {{ ref('stg_orders') }}

),

renamed as (

    select

        ---------- order details
        orderkey,
        custkey,
        orderstatus,
        totalprice,
        orderdate,
        orderpriority,
        clerk,
        shippriority,
        comment

    from source

)

select * from renamed
