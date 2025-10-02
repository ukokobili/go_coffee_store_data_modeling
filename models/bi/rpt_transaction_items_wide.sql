-- Wide view for BI: one row per transaction item with friendly attributes
with
    sales as (  -- fact (keys + measures) from mart
        select * from {{ ref('fct_transactions_items') }}
    ),
    dates as (  -- date attributes (year, month, week, etc.)
        select date_pk, date_day, year, month_name, iso_week, iso_weekday, year_month
        from {{ ref('dim_dates') }}
    ),
    stores as (  -- store attributes (names/geo)
        select * from {{ ref('dim_stores') }}
    ),
    payments as (  -- payment method attributes
        select * from {{ ref('dim_payment_methods') }}
    ),
    users as (  -- user attributes
        select * from {{ ref('dim_users') }}
    ),

    vouchers as (  -- voucher attributes
        select * from {{ ref('dim_vouchers') }}
    ),

    menus as (  -- menu item attributes
        select * from {{ ref('dim_menu_items') }}
    )

select
    -- grain / ids
    sales.transaction_fk,
    sales.date_fk,
    sales.item_fk,

    -- date attrs
    dates.date_day as txn_date,
    dates.year,
    dates.month_name,
    dates.iso_week,
    dates.iso_weekday,
    dates.year_month,

    -- store attrs
    stores.store_name,
    stores.street,
    stores.postal_code,
    stores.city,
    stores.state,
    stores.latitude,
    stores.longitude,

    -- payment attrs
    payments.payment_method,
    payments.payment_category,

    -- menu item attrs
    menus.item_id,
    menus.menu_item_name,
    menus.menu_category,
    menus.menu_price,
    menus.is_seasonal,

    -- user attrs
    users.user_id,
    users.gender,
    users.birthdate,
    users.registered_at,

    -- voucher attrs
    vouchers.voucher_code,
    vouchers.discount_type,

    -- measures
    sales.quantity,
    sales.gross_total,
    sales.discount_amount,
    sales.net_total,

    -- metadata columns
    current_timestamp as loaded_at

from sales
left join dates on dates.date_pk = sales.date_fk
left join stores on stores.store_pk = sales.store_fk
left join payments on payments.payment_pk = sales.payment_fk
left join users on users.user_pk = sales.user_fk
left join vouchers on vouchers.voucher_pk = sales.voucher_fk
left join menus on menus.item_pk = sales.item_fk
