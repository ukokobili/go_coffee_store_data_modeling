with
    source as (select * from {{ source('go_coffee_shop', 'vouchers') }}),

    renamed as (

        select
            -- original columns
            cast(voucher_id as string) as voucher_id,
            voucher_code,
            discount_type,
            discount_value,
            date(valid_from) as valid_from,
            date(valid_to) as valid_to,
            -- metadata columns
            current_timestamp as loaded_at

        from source

    )

select *
from renamed
