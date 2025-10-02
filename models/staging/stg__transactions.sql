with
    source as (select * from {{ source('go_coffee_shop', 'transactions') }}),

    renamed as (

        select
            -- original columns
            transaction_id,
            cast(store_id as string) as store_id,
            cast(payment_method_id as string) as payment_method_id,
            cast(voucher_id as string) as voucher_id,
            cast(user_id as string) as user_id,
            original_amount as gross_total,
            discount_applied,
            final_amount as net_total,
            date(created_at) as created_at,
            -- metadata columns
            current_timestamp as loaded_at

        from source

    ),

    -- sampled dataset
    sampled_data as ({{ limit_in_dev("select * from renamed") }})

select *
from sampled_data
