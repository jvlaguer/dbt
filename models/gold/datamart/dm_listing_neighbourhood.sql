{{
    config(
        alias='dm_listing_neighbourhood'
    )
}}

WITH

table_dimensions AS (
    SELECT 
    l.listing_neighbourhood, 
    TO_CHAR(fm.scraped_date, 'MM/YYYY') as month_year, 
    l.has_availability, 
    h.host_id, 
    fm.price, 
    h.host_is_superhost,
    fm.review_scores_rating,
    fm.availability_30
    FROM {{ ref('g_fact_listing_metrics') }} fm
    LEFT JOIN {{ ref('g_dim_airbnb_listing') }} l on fm.listing_id = l.listing_id and fm.scraped_date::timestamp >= l.valid_from and fm.scraped_date::timestamp < coalesce(l.valid_to, '9999-12-31'::timestamp)
    LEFT JOIN {{ ref('g_dim_airbnb_host') }} h on fm.host_id = h.host_id and fm.scraped_date::timestamp >= h.valid_from and fm.scraped_date::timestamp < coalesce(h.valid_to, '9999-12-31'::timestamp)
),


table_active_mom_cnt_distinct_host  AS (

    SELECT
    listing_neighbourhood,
    month_year,
    COUNT(*) as cnt_listings
    FROM table_dimensions
    WHERE has_availability='t'
    GROUP BY listing_neighbourhood, month_year

),

table_active_pcnt_change_cnt_distinct_host  as (

    SELECT *,
    cnt_listings/LAG(cnt_listings, 1) OVER (PARTITION BY listing_neighbourhood ORDER BY month_year) * 100::float as pcnt_change
    from table_active_mom_cnt_distinct_host

),

table_inactive_mom_cnt_distinct_host  as (

    SELECT
    listing_neighbourhood,
    month_year,
    COUNT(*) as cnt_listings
    FROM table_dimensions
    WHERE has_availability<>'t'
    GROUP BY listing_neighbourhood, month_year

),

table_inactive_pcnt_change_cnt_distinct_host  as (

    SELECT *,
    cnt_listings/LAG(cnt_listings, 1) OVER (PARTITION BY listing_neighbourhood ORDER BY month_year) * 100::float as pcnt_change
    from table_inactive_mom_cnt_distinct_host

),

main as (
    SELECT
    listing_neighbourhood,
    month_year,
    SUM(CASE WHEN has_availability='t' THEN 1 ELSE 0 END)/COUNT(*) * 100 ::float as active_listing_rate,
    MIN(CASE WHEN has_availability='t' THEN price ELSE null END) as min_active_listing_price,
    MAX(CASE WHEN has_availability='t' THEN price ELSE null END) as max_active_listing_price,
    AVG(CASE WHEN has_availability='t' THEN price ELSE null END) as avg_active_listing_price,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN has_availability='t' THEN price ELSE null END) as median_active_listing_price,
    COUNT(DISTINCT host_id) as cnt_distinct_host,
    COUNT(DISTINCT CASE WHEN host_is_superhost='t' THEN host_id ELSE NULL END)/COUNT(DISTINCT host_id)::numeric(4) as superhost_rate,
    AVG(CASE WHEN has_availability='t' THEN review_scores_rating ELSE null END) as avg_active_listing_review_score_rating,
    SUM(CASE WHEN has_availability='t' THEN 30-availability_30 ELSE NULL END) as number_of_stays,
    AVG(CASE WHEN has_availability='t' THEN (30-availability_30)*price ELSE NULL END) as est_rev_per_active_listing
    FROM table_dimensions
    group by 1,2

)

SELECT 
m.listing_neighbourhood,
m.month_year,
m.active_listing_rate,
m.min_active_listing_price,
m.max_active_listing_price,
m.avg_active_listing_price,
m.median_active_listing_price,
m.cnt_distinct_host,
m.superhost_rate,
m.avg_active_listing_review_score_rating,
a.pcnt_change as active_listings_pcnt_change,
i.pcnt_change as inactive_listings_pcnt_change,
m.number_of_stays,
m.est_rev_per_active_listing
FROM main m
LEFT join table_active_pcnt_change_cnt_distinct_host a
ON m.listing_neighbourhood = a.listing_neighbourhood AND m.month_year = a.month_year
LEFT JOIN table_inactive_pcnt_change_cnt_distinct_host i
ON m.listing_neighbourhood = i.listing_neighbourhood AND m.month_year = i.month_year
ORDER BY listing_neighbourhood, month_year