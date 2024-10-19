{{
    config(
        alias='dm_property_type'
    )
}}

WITH 

table_dimensions AS (
    SELECT 
    l.property_type,
    l.room_type,
    l.accommodates,
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


table_active_mom_cnt_distinct_host  as (

    SELECT
    property_type,
    room_type,
    accommodates,
    month_year,
    COUNT(*) as cnt_listings
    FROM table_dimensions
    WHERE has_availability='t'
    GROUP BY 1,2,3,4

),

table_active_pcnt_change_cnt_distinct_host  as (

    SELECT *,
    (cnt_listings::float/LAG(cnt_listings, 1) OVER (PARTITION BY property_type, room_type, accommodates ORDER BY SUBSTRING(month_year, 4, 4), SUBSTRING(month_year, 1, 2))::float - 1.0) * 100.0 as pcnt_change
    from table_active_mom_cnt_distinct_host

),

table_inactive_mom_cnt_distinct_host  as (

    SELECT
    property_type,
    room_type,
    accommodates,
    month_year,
    COUNT(*) as cnt_listings
    FROM table_dimensions
    WHERE has_availability<>'t'
    GROUP BY 1,2,3,4

),

table_inactive_pcnt_change_cnt_distinct_host  as (

    SELECT *,
    (cnt_listings::float/LAG(cnt_listings, 1) OVER (PARTITION BY property_type, room_type, accommodates ORDER BY SUBSTRING(month_year, 4, 4), SUBSTRING(month_year, 1, 2))::float - 1.0) * 100.0 as pcnt_change
    from table_inactive_mom_cnt_distinct_host

),

main as (
    SELECT
    property_type,
    room_type,
    accommodates,
    month_year,
    SUM(CASE WHEN has_availability='t' THEN 1 ELSE 0 END)/COUNT(*) * 100.0 as active_listing_rate,
    MIN(CASE WHEN has_availability='t' THEN price ELSE null END) as min_active_listing_price,
    MAX(CASE WHEN has_availability='t' THEN price ELSE null END) as max_active_listing_price,
    AVG(CASE WHEN has_availability='t' THEN price ELSE null END) as avg_active_listing_price,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN has_availability='t' THEN price ELSE null END) as median_active_listing_price,
    COUNT(DISTINCT host_id) as cnt_distinct_host,
    COUNT(DISTINCT CASE WHEN host_is_superhost='t' THEN host_id ELSE NULL END)::float/COUNT(DISTINCT host_id)::float as superhost_rate,
    AVG(CASE WHEN has_availability='t' THEN review_scores_rating ELSE null END) as avg_active_listing_review_score_rating,
    SUM(30-availability_30) as number_of_stays,
    AVG(CASE WHEN has_availability='t' THEN (30.0-availability_30::float)*price::float ELSE NULL END) as est_rev_per_active_listing
    FROM table_dimensions
    group by 1,2,3,4

)

SELECT 
m.property_type,
m.room_type,
m.accommodates,
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
ON m.property_type = a.property_type AND m.room_type = a.room_type AND m.accommodates = a.accommodates AND m.month_year = a.month_year
LEFT JOIN table_inactive_pcnt_change_cnt_distinct_host i
ON m.property_type = i.property_type AND m.room_type = i.room_type AND m.accommodates = i.accommodates AND m.month_year = i.month_year
ORDER BY m.property_type, m.room_type, m.accommodates, SUBSTRING(m.month_year, 4, 4), SUBSTRING(m.month_year, 1, 2)