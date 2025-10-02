with int_transactions as (
    select * from {{ ref('int_transactions_aggregated_with_discounts') }}
),

-- Step 1: Global transaction sequence
global_seq as (
    select
        transaction_id,
        row_number() over (order by created_at, transaction_id) as transaction_seq
    from int_transactions
),

-- Step 2: Customer-level sequences & classifications
customer_seq as (
    select
        transaction_id,
        row_number() over (
            partition by user_id order by created_at, transaction_id
        ) as customer_sales_seq,
        case
            when row_number() over (
                partition by user_id order by created_at, transaction_id
            ) = 1
            then 'new' else 'return'
        end as nvsr,
        min(created_at) over (partition by user_id) as fdos
    from int_transactions
),

-- Step 3: Lifetime value
lifetime_value as (
    select
        transaction_id,
        sum(net_total) over (
            partition by user_id order by created_at, transaction_id
        ) as customer_lifetime_value
    from int_transactions
)

-- Step 4: Final aggregation
select
    {{ dbt_utils.generate_surrogate_key(['t.transaction_id']) }} as transaction_pk,
    {{ dbt_utils.generate_surrogate_key(["t.user_id"]) }} as user_fk,
    {{ dbt_utils.generate_surrogate_key(["t.store_id"]) }} as store_fk,
    {{ dbt_utils.generate_surrogate_key(["t.payment_method_id"]) }} as payment_fk,
    {{ dbt_utils.generate_surrogate_key(["t.voucher_id"]) }} as voucher_fk,
    {{ dbt_utils.generate_surrogate_key(["t.created_at"]) }} as date_fk,

    g.transaction_seq,
    c.customer_sales_seq,
    c.nvsr,
    lv.customer_lifetime_value,
    c.fdos,

    t.quantity,
    t.gross_total,
    t.discount_applied,
    t.net_total,
    current_timestamp as loaded_at
from int_transactions t
left join global_seq g using (transaction_id)
left join customer_seq c using (transaction_id)
left join lifetime_value lv using (transaction_id)
