with
    source as (select * from {{ source('go_coffee_shop', 'transaction_items') }}),

    renamed as (

        select
            -- original columns
            transaction_id,
            cast(item_id as string) as item_id,
            quantity,
            unit_price,
            subtotal,
            date(created_at) as created_at,
            -- metadata columns
            current_timestamp as loaded_at

        from source

    ),

    -- sampled dataset
    sampled_data as ({{ limit_in_dev("select * from renamed") }})

select *
from sampled_data
