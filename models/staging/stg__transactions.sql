with source as (

    select * from {{ source('go_coffee_shop', 'transactions') }}

),

renamed as (

    select
        transaction_id,
        store_id,
        payment_method_id,
        voucher_id,
        user_id,
        original_amount,
        discount_applied,
        final_amount,
        created_at

    from source

)

select * from renamed

