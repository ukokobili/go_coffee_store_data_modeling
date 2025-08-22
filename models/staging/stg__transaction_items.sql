with source as (

    select * from {{ source('go_coffee_shop', 'transaction_items') }}

),

renamed as (

    select
        transaction_id,
        item_id,
        quantity,
        unit_price,
        subtotal,
        created_at

    from source

)

select * from renamed

