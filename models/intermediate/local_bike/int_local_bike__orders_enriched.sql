with orders_info as (

    select 

        order_id,
        customer_id,
        order_status,
        order_date,
        required_date,
        shipped_date,
        store_id,
        staff_id
        
    from  {{ref('stg_local_bike__orders')}}
),

orders_details as (

select 

        order_id, 
        count (distinct item_id) as nb_distinct_items, 
        count (distinct product_id) as nb_distinct_products, 
        sum(quantity) as quantity,
        sum(amount_without_discount) as total_amount,
        sum(amount_with_discount) as total_amount_discounted,
        sum(discounted_amount) as discounted_amount

from {{ref('int_local_bike__order_items_enriched')}}
group by order_id 

),

store_info as (

    select 

        store_id,
        store_name,
        city,
        state
    
    from {{ref('stg_local_bike__stores')}}
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

    orders_info.order_id,
    orders_info.customer_id,
    orders_info.order_status,
    orders_info.order_date,
    orders_info.required_date,
    orders_info.shipped_date,
    orders_info.store_id,
    store_info.store_name,
    store_info.city,
    store_info.state,
    orders_info.staff_id,
    staff_info.full_name,
    staff_info.active,
    orders_details.nb_distinct_items,
    orders_details.nb_distinct_products,
    orders_details.quantity,
    round(orders_details.total_amount,1) as total_amount,
    round(orders_details.total_amount_discounted,1) as total_amount_discounted,
    round(orders_details.discounted_amount,1) as discounted_amount


 from orders_info
 inner join orders_details 
    on orders_info.order_id = orders_details.order_id
 left join store_info
    on orders_info.store_id = store_info.store_id 
left join staff_info 
    on orders_info.staff_id = staff_info.staff_id
    and orders_info.store_id = staff_info.store_id
    

