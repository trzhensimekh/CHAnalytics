with 

customers as (
  select * 
  from {{ ref('stg_jaffle_shop__customers') }}
),

orders as (
  select * 
  from {{ ref('stg_jaffle_shop__orders') }}
),

payments as (
  select
    o.customer_id,
    p.order_id,
    sum(p.amount) as payment_amount
  from {{ ref('stg_stripe__payments') }} p
  join orders o using(order_id)
  group by 
    o.customer_id, 
    p.order_id
),

customer_orders as (
  select
    customer_id,
    min(order_date) as first_order_date,
    max(order_date) as most_recent_order_date,
    count(order_id)  as number_of_orders
  from orders
  group by customer_id
),

lifetime as (
  select
    customer_id,
    sum(payment_amount) as lifetime_value
  from payments
  group by customer_id
),

final as (
  select
    c.customer_id,
    c.first_name,
    c.last_name,
    co.first_order_date,
    co.most_recent_order_date,
    co.number_of_orders,
    coalesce(l.lifetime_value, 0) as lifetime_value
  from customers c
  left join customer_orders co using(customer_id)
  left join lifetime          l  using(customer_id)
)

select * 
from final
