with
    payment_methods as (select * from {{ ref('stg__payment_methods') }}),

    add_surrogate_keys as (
        select
            -- surrogate key
            {{ dbt_utils.generate_surrogate_key(['method_id']) }} as payment_pk,

            -- carry forward original fields
            method_id,
            payment_method,
            payment_category,
            -- metadata columns
            current_timestamp as loaded_at
        from payment_methods
    )
select *
from add_surrogate_keys
