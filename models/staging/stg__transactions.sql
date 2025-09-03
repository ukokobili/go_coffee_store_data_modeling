with
    source as (select * from {{ source('go_coffee_shop', 'transactions') }}),

    renamed as (

        select
            -- original columns
            transaction_id,
            store_id,
            payment_method_id,
            voucher_id,
            user_id,
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
