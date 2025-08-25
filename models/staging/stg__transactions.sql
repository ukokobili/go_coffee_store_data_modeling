with
    source as (select * from {{ source('go_coffee_shop', 'transactions') }}),

    renamed as (

        select
            transaction_id,
            store_id,
            payment_method_id,
            voucher_id,
            user_id,
            original_amount as gross_total,
            discount_applied,
            final_amount as net_total,
            created_at

        from source

    )

select *
from renamed
