with

customers as (

    select * from {{ ref('stg_customers') }}
),

nation as (

    select * from {{ ref('stg_nation') }}
),

region as (

    select * from {{ ref('stg_region') }}
),

renamed as (

    select

        ---------- customer info
        c.customer_key,
        c.name as customer_name,
        c.address,
        c.phone,
        c.acctbal,
        c.mktsegment,

        ---------- nation info
        n.name as nation_name,
        n.comment as nation_comment,

        ---------- region info
        r.name as region_name,
        r.comment as region_comment

    from customers c
    left join nation n on c.nationkey = n.nationkey
    left join region r on n.regionkey = r.regionkey

)

select * from renamed
