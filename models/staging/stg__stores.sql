with
    source as (select * from {{ source('go_coffee_shop', 'stores') }}),

    renamed as (

        select
            store_id,
            replace(store_name, 'G Coffee @ USJ 89q', '') as store_name,
            street,
            postal_code,
            city,
            state,
            latitude,
            longitude

        from source

    )

select *
from renamed
