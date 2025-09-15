with
    -- bring in the transaction-grain fact (gross/discount/net already computed)
    int_transactions as (
        select * from {{ ref('int_transactions_aggregated_with_discounts') }}
    ),
    -- derive keys, sequences, customer status, and lifetime value
    aggregation as (
        select
            -- surrogate key for this fact table
            {{ dbt_utils.generate_surrogate_key(['transaction_id']) }}
            as transaction_pk,
            {{ dbt_utils.generate_surrogate_key(["user_id"]) }} as user_fk,
            {{ dbt_utils.generate_surrogate_key(["store_id"]) }} as store_fk,
            {{ dbt_utils.generate_surrogate_key(["payment_method_id"]) }} as payment_fk,
            {{ dbt_utils.generate_surrogate_key(["voucher_id"]) }} as voucher_fk,
            {{ dbt_utils.generate_surrogate_key(["created_at"]) }} as date_fk,

            -- sales transaction sequence across the whole business (by time, then id
            -- to break ties)
            row_number() over (order by created_at, transaction_id) as transaction_seq,

            -- per-customer purchase sequence (first order = 1)
            row_number() over (
                partition by user_id order by created_at, transaction_id
            ) as customer_sales_seq,

            -- classify first purchase vs subsequent purchases
            case
                when
                    (
                        rank() over (
                            partition by user_id order by created_at, transaction_id
                        )
                        = 1
                    )
                then 'new'
                else 'return'
            end as nvsr,

            -- first day of sale (first order timestamp for this customer)
            first_value(created_at) over (
                partition by user_id order by created_at, transaction_id
            ) as fdos,

            -- carry forward core transaction measures
            quantity,
            gross_total,
            discount_applied,
            net_total,
            -- metadata columns
            current_timestamp as loaded_at
        from int_transactions
    )

-- final output of this model (enriched transaction facts)
select *
from aggregation
