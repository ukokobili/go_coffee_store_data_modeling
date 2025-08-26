with
    source as (select * from {{ source('go_coffee_shop', 'payment_methods') }}),

    renamed as (

        select
            -- rename columns to be more friendly
            method_id,

            -- replace underscores with spaces in method_name
            replace(method_name, '_', ' ') as method_name,
            category as category,
            -- metadata columns
            current_timestamp as loaded_at

        from source

    )

select *
from renamed
