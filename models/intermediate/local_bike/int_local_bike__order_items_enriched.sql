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

    from {{ref('stg_local_bike__order_items')}}
),

product_details as (

select

    *

 from {{ref('int_local_bike__products_enriched')}}
)

select 

    orders.*, 
    product_details.brand_id,
    product_details.brand_name,
    product_details.category_id,
    product_details.category_name,
    product_details.model_year

from orders
left join product_details 
    on orders.product_id = product_details.product_id 



