with

source as (

    select * from {{ source('raw_layer', 'region') }}

),

renamed as (

    select

        ---------- ids
        r_regionkey as regionkey,

        ---------- text
        r_name as name,
        r_comment as comment

    from source

)

select * from renamed
