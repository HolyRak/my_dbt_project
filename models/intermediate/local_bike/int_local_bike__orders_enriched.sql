with orders_info as (

    select 

        order_id,
        customer_id,
        order_status,
        order_date,
        required_date,
        shipped_date,
        store_id,
        store_name,
        city,
        state,
        staff_id,
        full_name,
        active, 
        count (distinct item_id) as nb_distinct_items, 
        count (distinct product_id) as nb_distinct_products, 
        sum(quantity) as quantity,
        sum(amount_without_discount) as total_amount,
        sum(amount_with_discount) as total_amount_discounted,
        sum(discounted_amount) as discounted_amount

from {{ref('int_local_bike__order_items_enriched')}}
group by 1,2,3,4,5,6,7,8,9,10,11,12,13

)

select 

    order_id,
    customer_id,
    order_status,
    order_date,
    required_date,
    shipped_date,
    store_id,
    store_name,
    city,
    state,
    staff_id,
    full_name,
    active,
    nb_distinct_items,
    nb_distinct_products,
    quantity,
    round(total_amount,1) as total_amount,
    round(total_amount_discounted,1) as total_amount_discounted,
    round(discounted_amount,1) as discounted_amount

 from orders_info
