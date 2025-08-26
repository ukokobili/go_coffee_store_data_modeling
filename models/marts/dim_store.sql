with
    stores as (select * from {{ ref('stg__stores') }}),
    add_surrogate_keys as (
        select
            -- surrogate key
            {{ dbt_utils.generate_surrogate_key(['store_id']) }} as store_pk,
            -- carry forward original fields
            store_id,
            store_name,
            street,
            postal_code,
            city,
            state,
            latitude,
            longitude,
            -- metadata columns
            current_timestamp as loaded_at
        from stores
    )
select *
from add_surrogate_keys
