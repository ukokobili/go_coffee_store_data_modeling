with
    source as (select * from {{ source('go_coffee_shop', 'users') }}),

    renamed as (select
    -- original columns
     user_id, gender, birthdate, registered_at,
            -- metadata columns
            current_timestamp as loaded_at from source)

select *
from renamed
