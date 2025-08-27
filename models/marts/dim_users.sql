with
    users as (select * from {{ ref('stg__users') }}),
    add_surrogate_keys as (
        select
            -- surrogate key
            {{ dbt_utils.generate_surrogate_key(['user_id']) }} as user_pk,
            -- carry forward original fields
            user_id,
            gender,
            birthdate,
            registered_at,
            -- metadata columns
            current_timestamp as loaded_at
        from users
    )

select *
from add_surrogate_keys
