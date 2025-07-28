{{ config(materialized='view') }}

with monthly_transactions as (
    select
        cp.customer_id,
        date_trunc('month', t.txn_date) as transaction_month,
        count(t.txn_id) as monthly_transaction_count
    from
        {{ ref('credit_card_transactions') }} as t
    join
        {{ ref('customer_products') }} as cp on t.customer_product_id = cp.customer_product_id
    group by
        cp.customer_id,
        transaction_month
)

select
    customer_id,
    transaction_month,
    monthly_transaction_count,
    sum(monthly_transaction_count) over (partition by customer_id order by transaction_month) as running_total_of_transactions
from
    monthly_transactions