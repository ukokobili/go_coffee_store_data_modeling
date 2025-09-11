with
    source as (select * from {{ source('go_coffee_shop', 'menu_items') }}),

    renamed as (

        select
            -- original columns
            item_id,
            item_name as menu_item_name,
            case
                when category = 'coffee' then 'Coffee' else 'Non-Coffee'
            end as menu_category,
            price as menu_price,
            is_seasonal,
            available_from,
            available_to,
            -- metadata columns
            current_timestamp as loaded_at

        from source

    )

select *
from renamed
