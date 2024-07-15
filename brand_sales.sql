{{
  config(
    materialized='table'
  )
}}

with categorized_brands as (
    select 
        brand_id,
        brand_name,
        selling_price,
        quanity,
        {{ generate_category_case('brand_name') }} as category_id,
        {{ profit('selling_price', 'quanity') }} as brand_profits
        
    from 
        {{ ref('stg_brands') }}
),
brand_sales as (
    select
        b.brand_id,
        b.brand_name,
        b.selling_price as brand_selling_price,
        b.quanity,
        b.category_id,
        c.category_name,
        c.category_quantity,
        b.brand_profits,
        (b.selling_price * c.category_quantity) as bc_sales,
        sum((b.selling_price * c.category_quantity)) over () as total_sales
    from 
        categorized_brands b
    join 
        {{ ref('stg_categories') }} c
    on 
        b.category_id = c.category_id
)

select
    brand_id,
    brand_name,
    quanity,
    brand_selling_price,
    category_id,
    category_name,
    category_quantity,
    brand_profits,
    bc_sales,
    total_sales,
   
from 
    brand_sales
order by 
    brand_name