with source as (

    select * from {{ source('go_coffee_shop', 'payment_methods') }}

),

renamed as (

    select
        method_id,
        method_name,
        category

    from source

)

select * from renamed

