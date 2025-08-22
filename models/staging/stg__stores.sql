with source as (

    select * from {{ source('go_coffee_shop', 'stores') }}

),

renamed as (

    select
        store_id,
        store_name,
        street,
        postal_code,
        city,
        state,
        latitude,
        longitude

    from source

)

select * from renamed

