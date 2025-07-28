{{
    config(
        materialized='incremental',
        unique_key=['customer_id', 'month', 'txn_type']
    )
}}

with transactions as (
    select
        cp.customer_id,
        t.txn_type,
        t.txn_date,
        t.amount
    from {{ ref('credit_card_transactions') }} t
    join {{ ref('customer_products') }} cp on t.customer_product_id = cp.customer_product_id
    {% if is_incremental() %}
      -- This filter will only be applied on an incremental run
      where t.txn_date > (select max(month) from {{ this }})
    {% endif %}
)

select
    customer_id,
    txn_type,
    date_trunc('month', txn_date) as month,
    sum(amount) as monthly_amount
from transactions
group by 1, 2, 3