{{
  config(
    materialized = 'ephemeral',
    )
}}

with
    transactions as (
        select * 
        from {{ ref('stg__transactions') }}
    ),

    items as (
        select
            transaction_id, 
            sum(quantity) as quantity
        from {{ ref('stg__transaction_items') }} 
        group by 1
    ),

    aggregated as (
        select 
            transactions.*, 
            coalesce(items.quantity, 0) as quantity
        from transactions
        left join items using (transaction_id)
    )
select *
from aggregated
