with
    vouchers as (select * from {{ ref('stg__vouchers') }}),

    add_surrogate_keys as (
        select
            -- surrogate key
            {{ dbt_utils.generate_surrogate_key(['voucher_id']) }} as voucher_pk,

            -- natural key
            voucher_id,
            voucher_code,
            discount_type,
            discount_value,
            valid_from,
            valid_to,
            -- metadata columns
            current_timestamp as loaded_at
        from vouchers
    )
select *
from add_surrogate_keys
