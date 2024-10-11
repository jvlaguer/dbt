{{
    config(
        alias='monthly_aggregation_by_sub_category'
    )
}}

select
date_trunc('month', date)::date as month,
b.sub_category_description,
count(distinct order_id) as total_orders,
sum(price) as total_sales
from {{ ref('g_fact_orders') }} a
left join {{ ref('g_dim_sub_category') }} b  on a.sub_category_id = b.sub_category_id and a.date::timestamp >= b.valid_from and a.date::timestamp < coalesce(b.valid_to, '9999-12-31'::timestamp)
group by 1,2