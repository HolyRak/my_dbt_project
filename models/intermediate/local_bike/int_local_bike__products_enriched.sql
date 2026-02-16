with brands as (
    
      select 

            *
    
    from {{ref('stg_local_bike__brands')}}

),

categories as (
    
      select 

            *
    
    from {{ref('stg_local_bike__categories')}}

),

products as (
    
      select 

            *
    
    from {{ref('stg_local_bike__products')}}

)


select 

    products.product_id, 
    products.brand_id,
    brands.brand_name,
    products.category_id,
    categories.category_name,
    products.model_year,
    products.list_price 

from products
left join brands 
    on products.brand_id = brands.brand_id
left join categories
    on products.category_id = categories.category_id 


