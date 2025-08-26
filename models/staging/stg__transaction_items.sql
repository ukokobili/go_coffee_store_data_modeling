with
    source as (select * from {{ source('go_coffee_shop', 'transaction_items') }}),

    renamed as (

        select
            -- original columns
            transaction_id,
            item_id,
            quantity,
            unit_price,
            subtotal,
            date(created_at) as created_at,
            -- metadata columns
            current_timestamp as loaded_at

        from source

    )

select *
from renamed
