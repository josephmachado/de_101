with

source as (

    select * from {{ ref('stg_lineitems') }}

),

renamed as (

    select

        ---------- line item details
        orderkey,
        partkey,
        suppkey,
        linenumber,
        quantity,
        extendedprice,
        discount,
        tax,
        returnflag,
        linestatus,
        shipdate,
        commitdate,
        receiptdate,
        shipinstruct,
        shipmode,
        comment

    from source

)

select * from renamed
