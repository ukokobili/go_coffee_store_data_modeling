with
    menu_items as (select * from {{ ref('stg__menu_items') }}),
    add_surrogate_keys as (

        select
            -- surrogate key
            {{ dbt_utils.generate_surrogate_key(['item_id']) }} as item_pk,
            -- original columns
            item_id, 
            menu_item_name,
            menu_category,
            menu_price,
            is_seasonal,
            available_from,
            available_to,
            -- metadata columns
            current_timestamp as loaded_at
        from menu_items
    )
select *
from add_surrogate_keys
