with product_details as (

    select

        product_id,
        category_id,
        category_name,
        brand_id,
        brand_name, 
        model_year,
        sum(quantity) as quantity_sold, 
        store_id,
        stock,
        round(sum(amount_without_discount),1) as amount_sold_without_discount,
        round(sum(amount_with_discount),1) as amount_sold_with_discount,
        round(sum(amount_without_discount) -sum(amount_with_discount),1) as amount_discounted

    from {{ref('int_local_bike__order_items_enriched')}}
    group by 1,2,3,4,5,6,8,9
)

select 

  product_id,
  store_id,
  stock,
  category_id,
  category_name, 
  brand_id,
  brand_name,
  model_year,
  quantity_sold,
  amount_sold_without_discount,
  amount_sold_with_discount,
  amount_discounted

from product_details