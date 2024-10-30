with

source as (

    select * from {{ source('raw_layer', 'lineitem') }}

),

renamed as (

    select

        ---------- ids
        l_orderkey as orderkey,
        l_partkey as partkey,
        l_suppkey as suppkey,
        l_linenumber as linenumber,

        ---------- quantities and pricing
        l_quantity as quantity,
        l_extendedprice as extendedprice,
        l_discount as discount,
        l_tax as tax,

        ---------- status and flags
        l_returnflag as returnflag,
        l_linestatus as linestatus,

        ---------- dates
        l_shipdate as shipdate,
        l_commitdate as commitdate,
        l_receiptdate as receiptdate,

        ---------- shipping details
        l_shipinstruct as shipinstruct,
        l_shipmode as shipmode,

        ---------- comments
        l_comment as comment

    from source

)

select * from renamed
