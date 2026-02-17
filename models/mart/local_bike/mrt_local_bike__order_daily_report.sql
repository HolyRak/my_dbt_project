select 

  order_date,
  order_status,
  shipped_date IS NOT NULL AS is_shipped,
  store_id, 
  store_name,
  city,
  state,
  staff_id,
  full_name,
  active, 
  count (distinct order_id) as nb_orders,
  sum(quantity) as quantity,
  round(sum(total_amount),1) as total_amount,
  round(sum(total_amount_discounted),1) as total_amount_discounted,
  round(sum(total_amount) -sum(total_amount_discounted),1) as amount_discounted
from {{ref('int_local_bike__orders_enriched')}}
group by 1,2,3,4,5,6,7,8,9,10
order by order_date
