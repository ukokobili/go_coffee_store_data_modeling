with
    transaction_items as (select * from {{ ref('int_transaction_items_with_net') }}),

    add_surrogate_keys as (
        select

            {{ dbt_utils.generate_surrogate_key(['transaction_id']) }}
            as transaction_fk,
            {{ dbt_utils.generate_surrogate_key(["user_id"]) }} as user_fk,
            {{ dbt_utils.generate_surrogate_key(["store_id"]) }} as store_fk,
            {{ dbt_utils.generate_surrogate_key(["payment_method_id"]) }} as payment_fk,
            {{ dbt_utils.generate_surrogate_key(["item_id"]) }} as item_fk,
            {{ dbt_utils.generate_surrogate_key(["voucher_id"]) }} as voucher_fk,
            {{ dbt_utils.generate_surrogate_key(["created_at"]) }} as date_fk,

            -- carry forward core item measures
            transaction_id,
            quantity,
            gross_total,
            discount_amount,
            net_total,
            -- metadata columns
            current_timestamp as loaded_at
        from transaction_items
    )
select *
from add_surrogate_keys
