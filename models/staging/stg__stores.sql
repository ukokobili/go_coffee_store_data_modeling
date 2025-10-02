with
    source as (select * from {{ source('go_coffee_shop', 'stores') }}),

    renamed as (

        select
            -- original columns
            cast(store_id as string) as store_id,
            initcap(replace(store_name, 'G Coffee @', '')) as store_name,
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
