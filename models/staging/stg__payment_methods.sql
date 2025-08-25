with
    source as (select * from {{ source('go_coffee_shop', 'payment_methods') }}),

    renamed as (

        select
            method_id,
            replace(method_name, '_', ' ') as method_name,
            category as category

        from source

    )

select *
from renamed
