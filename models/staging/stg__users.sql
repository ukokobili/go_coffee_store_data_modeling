with source as (

    select * from {{ source('go_coffee_shop', 'users') }}

),

renamed as (

    select
        user_id,
        gender,
        birthdate,
        registered_at

    from source

)

select * from renamed

