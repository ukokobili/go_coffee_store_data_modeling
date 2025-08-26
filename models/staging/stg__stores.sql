with
    source as (select * from {{ source('go_coffee_shop', 'stores') }}),

    renamed as (

        select
            -- original columns
            store_id,
            replace(store_name, 'G Coffee @ USJ 89q', '') as store_name,
            street,
            postal_code,
            city,
            state,
            latitude,
            longitude,
            -- metadata columns
            current_timestamp as loaded_at

        from source

    )

select *
from renamed
