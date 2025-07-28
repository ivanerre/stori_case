{{ config(materialized='view') }}

with joine_cte as (
    select
        c.customer_id,
        c.full_name,
        t.txn_type,
        t.amount
    from {{ ref('credit_card_transactions') }} t
    join {{ ref('customer_products') }} cp
        on t.customer_product_id = cp.customer_product_id
    join {{ ref('customers') }} c
        on cp.customer_id = c.customer_id
),

agg_output as (
    select
        customer_id,
        full_name,
        txn_type,
        sum(amount) as total_amount
    from joined_cte
    group by customer_id, full_name, txn_type
)

select * from agg_output