{{
    config(
        alias='dm_property_type'
    )
}}

with


table_active_mom_cnt_distinct_host  as (

    SELECT
    property_type,
    room_type,
    accommodates,
    TO_CHAR(scraped_date, 'MMM/YYYY') as month_year,
    COUNT(DISTINCT host_id) as cnt_distinct_host
    FROM {{ ref('g_fact_listing_metrics') }}
    WHERE has_availability='t'
    GROUP BY
    property_type,
    room_type,
    accommodates,
    TO_CHAR(scraped_date, 'MMM/YYYY') as month_year,
    ORDER BY TO_CHAR(scraped_date, 'MM/YYYY')

),

table_active_pcnt_change_cnt_distinct_host  as (

    SELECT *,
    LAG(cnt_distinct_host, 1) OVER (PARTITION BY property_type, room_type, accommodates ORDER BY month_year) as pcnt_change
    from table_active_mom_cnt_distinct_host

),

table_inactive_mom_cnt_distinct_host  as (

    SELECT
    property_type,
    room_type,
    accommodates,
    TO_CHAR(scraped_date, 'MMM/YYYY') as month_year,
    COUNT(DISTINCT host_id) as cnt_distinct_host
    FROM {{ ref('g_fact_listing_metrics') }}
    WHERE has_availability<>'t'
    GROUP BY 
    property_type,
    room_type,
    accommodates,
    TO_CHAR(scraped_date, 'MMM/YYYY') as month_year
    ORDER BY TO_CHAR(scraped_date, 'MM/YYYY')

),

table_inactive_pcnt_change_cnt_distinct_host  as (

    SELECT *,
    LAG(cnt_distinct_host, 1) OVER (PARTITION BY property_type, room_type, accommodates ORDER BY month_year) as pcnt_change
    from table_inactive_mom_cnt_distinct_host

),

main as (
    SELECT
    l.property_type,
    l.room_type,
    l.accommodates,
    TO_CHAR(fm.scraped_date, 'MMM/YYYY') as month_year,
    SUM(IF(l.has_availability='t',1,0))/COUNT(*) as active_listing_rate,
    MIN(IF(l.has_availability='t',fm.price,null)) as min_active_listing_price,
    MAX(IF(l.has_availability='t',fm.price,null)) as max_active_listing_price,
    AVG(IF(l.has_availability='t',fm.price,null)) as avg_active_listing_price,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fm.price) as median_active_listing_price,
    COUNT(DISTINCT fm.host_id) as cnt_distinct_host,
    COUNT(DISTINCT IF(h.host_is_superhost='t',fm.host_id,NULL))/COUNT(DISTINCT fm.host_id) as superhost_rate,
    AVG(IF(l.has_availability='t',fm.review_scores_rating,null)) as avg_active_listing_review_score_rating,
    SUM(30-fm.availability_30) as number_of_stays,
    AVG(IF(l.has_availability='t',(30-m.availability_30)*fm.price,NULL)) as est_rev_per_active_listing,
    from {{ ref('g_fact_listing_metrics') }} fm
    LEFT JOIN {{ ref('g_dim_airbnb_listing') }} l on fm.listing_id = l.listing_id and fm.scraped_date = l.scraped_date and fm.scraped_date::timestamp >= l.valid_from and fm.scraped_date::timestamp < coalesce(l.valid_to, '9999-12-31'::timestamp)
    LEFT JOIN {{ ref('g_dim_airbnb_host') }} h on fm.host_id = h.host_id and fm.scraped_date = h.scraped_date and fm.scraped_date::timestamp >= h.valid_from and fm.scraped_date::timestamp < coalesce(h.valid_to, '9999-12-31'::timestamp)
    group by 1,2,3,4

)

SELECT 
m.property_type,
m.room_type,
m.accommodates,
m.month_year
m.active_listing_rate,
m.min_active_listing_price,
m.max_active_listing_price,
m.avg_active_listing_price,
m.median_active_listing_price,
m.cnt_distinct_host,
m.superhost_rate,
m.avg_active_listing_review_score_rating,
a.pcnt_change,
i.pcnt_change,
m.number_of_stays,
m.est_rev_per_active_listing
FROM main m
LEFT join table_active_pcnt_change_cnt_distinct_host a
ON m.listing_neighbourhood = a.listing_neighbourhood AND m.month_year = a.month_year
LEFT JOIN table_inactive_pcnt_change_cnt_distinct_host i
ON m.listing_neighbourhood = i.listing_neighbourhood AND m.month_year = i.month_year
ORDER BY m.property_type, m.room_type, m.accommodates, m.month_year;