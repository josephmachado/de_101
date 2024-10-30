with

source as (

    select * from {{ source('raw_layer', 'nation') }}

),

renamed as (

    select

        ---------- ids
        n_nationkey as nationkey,

        ---------- text
        n_name as name,
        n_comment as comment,

        ---------- references
        n_regionkey as regionkey

    from source

)

select * from renamed
