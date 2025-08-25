with
    -- items-level facts from staging (one row per line item)
    transaction_items as (select * from {{ ref('stg__transaction_items') }}),

    -- transaction headers from staging (one row per transaction)
    transactions as (select * from {{ ref('stg__transactions') }}),

    -- voucher definitions, types, values, and validity windows
    vouchers as (select * from {{ ref('stg__vouchers') }}),

    -- re-grain — collapse items to transaction level and compute gross totals
    aggregate_items_to_transaction_grain as (
        select
            -- normalize to date for date-range comparisons against voucher windows
            date(transactions.created_at) as created_at,

            -- transaction keys / dimensions
            transactions.transaction_id,
            transactions.store_id,
            transactions.payment_method_id,
            transactions.voucher_id,
            transactions.user_id,

            -- metrics at transaction grain
            sum(transaction_items.quantity) as quantity,
            sum(transaction_items.subtotal) as gross_total
        from transaction_items
        left join transactions using (transaction_id) {{ dbt_utils.group_by(n=6) }}
    ),

    -- business rule — apply percentage/fixed discounts within valid windows
    apply_discounts as (
        select
            a.*,
            case
                when
                    a.voucher_id is not null
                    and a.txn_date between v.valid_from and v.valid_to
                then
                    case
                        when v.discount_type = 'percentage'
                        -- percentage discount = gross_total * (value/100)
                        then round(a.gross_total * (v.discount_value / 100.0), 2)
                        when v.discount_type = 'fixed'
                        -- fixed discount capped at gross_total so net never goes
                        -- negative
                        then least(a.gross_total, v.discount_value)
                        else 0
                    end
                else 0
            end as discount_amount
        from aggregate_items_to_transaction_grain a
        left join vouchers v using (voucher_id)
    )

-- expose transaction-level totals and net after discount
select *, greatest(0, gross_total - discount_amount) as net_total
from apply_discounts
