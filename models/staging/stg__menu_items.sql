with source as (

    select * from {{ source('go_coffee_shop', 'menu_items') }}

),

renamed as (

    select
        item_id,
        item_name,
        category,
        price,
        is_seasonal,
        available_from,
        available_to

    from source

)

select * from renamed

