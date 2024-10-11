{{
    config(
        alias='dm_listing_neighbourhood'
    )
}}
WITH table_duration_median as (
  SELECT DISTINCT 'green' as taxi_type, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY duration_min) OVER() AS median_duration
  FROM taxi_data
),

SELECT
listing_neighbourhood,
TO_CHAR(scraped_date, 'MMM/YYYY') as month,
SUM(IF(has_availability='t',1,0))/COUNT(*) as active_listing_rate,
MIN(IF(has_availability='t',price,null)) as min_active_listing_price,
MAX(IF(has_availability='t',price,null)) as max_active_listing_price,
AVG(IF(has_availability='t',price,null)) as avg_active_listing_price,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) as median_active_listing_price,
COUNT(DISTINCT host_id) as cnt_distinct_host,
COUNT(DISTINCT IF(host_is_superhost='t',host_id,NULL))/COUNT(DISTINCT host_id) as superhost_rate,
AVG(IF(has_availability='t',review_scores_rating,null)) as avg_active_listing_review_score_rating,
--
SUM(30-availability_30) as number_of_stays,
AVG(IF(has_availability='t',(30-availability_30)*price,NULL)) as est_rev_per_active_listing,


from {{ ref('g_fact_orders') }} a
left join {{ ref('g_dim_category') }} b  on a.category_id = b.category_id and a.date::timestamp >= b.valid_from and a.date::timestamp < coalesce(b.valid_to, '9999-12-31'::timestamp)
group by 1,2