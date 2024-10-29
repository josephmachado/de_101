with

source as (

    select * from {{ source('tpch', 'customer') }}

),

renamed as (

    select

        ---------- ids
        c_custkey as customer_key,

        ---------- text
        c_name as name,
        c_address as address,

        ---------- references
        c_nationkey as nationkey,

        ---------- contact
        c_phone as phone,

        ---------- financials
        c_acctbal as acctbal,

        ---------- segment
        c_mktsegment as mktsegment,

        ---------- comments
        c_comment as comment

    from source

)

select * from renamed
