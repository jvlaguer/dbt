{{
    config(
        unique_key='order_id',
        alias='fact_orders'
    )
}}

select
	order_id,
	date,
	case when brand_id in (select distinct brand_id from {{ ref('g_dim_brand') }}) then brand_id else 0 end as brand_id,
	case when category_id in (select distinct category_id from {{ ref('g_dim_category') }}) then category_id else 0 end as category_id,
	case when sub_category_id in (select distinct sub_category_id from {{ ref('g_dim_sub_category') }}) then sub_category_id else 0 end as sub_category_id,
	price
from {{ ref('s_orders') }}