with customers as (
    select * from {{ ref('stg_jaffle_shop__customers') }}

),
orders as (
    select * from {{ ref('stg_jaffle_shop__orders') }}
    join customers using(customer_id)
),
payments as (
    select * from {{ ref('stg_stripe__payments') }}
),
final as (
    select
        orders.order_id,
        orders.customer_id,
        payments.amount
    from payments 
    join orders using (order_id)
)
select * from final