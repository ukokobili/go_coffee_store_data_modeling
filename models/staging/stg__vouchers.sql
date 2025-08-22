with source as (

    select * from {{ source('go_coffee_shop', 'vouchers') }}

),

renamed as (

    select
        voucher_id,
        voucher_code,
        discount_type,
        discount_value,
        valid_from,
        valid_to

    from source

)

select * from renamed

