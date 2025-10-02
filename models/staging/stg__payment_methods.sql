with
    source as (select * from {{ source('go_coffee_shop', 'payment_methods') }}),

    renamed as (

        select
            -- rename columns to be more friendly
            cast(method_id as string) as method_id,

            -- original column name is 'category', but renaming to 'payment_method'
            -- for clarity
            case
                when method_name = 'credit_card'
                then 'Credit Card'
                when method_name = 'debit_card'
                then 'Debit Card'
                when method_name = 'tng'
                then 'TNG'
                when method_name = 'grabpay'
                then 'Grab Pay'
                else 'Cash'
            end as payment_method,

            -- categorize payment methods into broader categories
            case
                when category = 'cash'
                then 'Cash'
                when category = 'card'
                then 'Card'
                else 'E-wallet'
            end as payment_category,
            -- metadata columns
            current_timestamp as loaded_at

        from source

    )

select *
from renamed
