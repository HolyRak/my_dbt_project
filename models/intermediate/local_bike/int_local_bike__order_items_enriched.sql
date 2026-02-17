
with orders as (
    
      select 

        order_item_id, 
        order_id,
        item_id, 
        product_id, 
        quantity,
        list_price,
        discount,
        quantity * list_price as amount_without_discount,
        quantity * list_price * discount as discounted_amount,
        quantity * list_price * (1 - discount) as amount_with_discount

    from  {{ref('stg_local_bike__order_items')}}
),

orders_info as (

    select

      order_id, 
      customer_id,
      order_status,
      order_date,
      required_date, 
      shipped_date,
      store_id,
      staff_id

    from {{ref('stg_local_bike__orders')}}
  
),

product_details as (

select

    *

 from {{ref('int_local_bike__products_enriched')}}

),

store_info as (

    select 

        store_id,
        store_name,
        city, 
        state

 from {{ref('stg_local_bike__stores')}}

),

stocks_info as (

    select 

      store_id,
      product_id,
      quantity as stock

    from {{ref('stg_local_bike__stocks')}}

),


staff_info as (

    select

        staff_id,
        CONCAT(first_name, ' ', last_name) AS full_name,
        active,
        store_id

    from {{ref('stg_local_bike__staffs')}}

)

select 

    orders_info.order_date,
    orders.order_item_id, 
    orders.order_id,
    orders_info.customer_id,
    orders_info.order_status,
    orders_info.required_date,
    orders_info.shipped_date,
    orders_info.store_id,
    store_info.city,
    store_info.state,
    store_info.store_name,
    orders_info.staff_id,
    staff_info.active, 
    staff_info.full_name,
    orders.item_id, 
    orders.product_id, 
    stocks_info.stock,
    orders.quantity,
    orders.list_price,
    orders.discount, 
    orders.amount_without_discount, 
    orders.discounted_amount, 
    orders.amount_with_discount, 
    product_details.brand_id,
    product_details.brand_name,
    product_details.category_id,
    product_details.category_name,
    product_details.model_year

from orders
left join product_details 
    on orders.product_id = product_details.product_id 
left join orders_info
  on orders.order_id = orders_info.order_id 
left join store_info 
  on store_info.store_id = orders_info.store_id 
left join stocks_info 
  on stocks_info.store_id = orders_info.store_id 
  and stocks_info.product_id = orders.product_id
left join staff_info
    on orders_info.staff_id = staff_info.staff_id 
    and orders_info.store_id = staff_info.store_id 

