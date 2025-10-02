with
    source as (select * from {{ source('go_coffee_shop', 'users') }}),

    renamed as (
        select
            -- original columns
            cast(user_id as string) as user_id,
            initcap(gender) as gender,
            birthdate,
            registered_at,
            -- metadata columns
            current_timestamp as loaded_at
        from source
    ),

    -- sampled dataset
    sampled_data as ({{ limit_in_dev("select * from renamed") }})

select *
from sampled_data
