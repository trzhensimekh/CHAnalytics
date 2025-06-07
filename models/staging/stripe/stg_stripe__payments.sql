select
    id as payment_id,
    orderid as order_id,
    PAYMENTMETHOD,
    STATUS,
    AMOUNT,
    CREATED,
from {{ source('stripe', 'payment') }}