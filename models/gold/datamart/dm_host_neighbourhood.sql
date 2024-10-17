{{
    config(
        alias='dm_host_neighbourhood'
    )
}}

SELECT
s.lga_name as host_neighbourhood_lga,
TO_CHAR(fm.scraped_date, 'MM/YYYY') as month_year,
COUNT(DISTINCT h.host_id) as cnt_distinct_host,
SUM(CASE WHEN l.has_availability='t' THEN (30-fm.availability_30)*fm.price ELSE NULL END) as est_rev_per_active_listing,
SUM(CASE WHEN l.has_availability='t' THEN (30-fm.availability_30)*fm.price ELSE NULL END)/COUNT(DISTINCT h.host_id) as est_rev_per_active_listing_per_host
FROM {{ ref('g_fact_listing_metrics') }} fm
LEFT JOIN {{ ref('g_dim_airbnb_listing') }} l on fm.listing_id = l.listing_id and fm.scraped_date::timestamp >= l.valid_from and fm.scraped_date::timestamp < coalesce(l.valid_to, '9999-12-31'::timestamp)
LEFT JOIN {{ ref('g_dim_airbnb_host') }} h on fm.host_id = h.host_id and fm.scraped_date::timestamp >= h.valid_from and fm.scraped_date::timestamp < coalesce(h.valid_to, '9999-12-31'::timestamp)
LEFT JOIN {{ ref('g_dim_lga_suburb') }} s on lower(h.host_neighbourhood) = lower(s.suburb_name) and fm.scraped_date::timestamp >= s.valid_from and fm.scraped_date::timestamp < coalesce(s.valid_to, '9999-12-31'::timestamp)
GROUP BY 1,2
ORDER BY 1,SUBSTRING(TO_CHAR(fm.scraped_date, 'MM/YYYY'),4,4), SUBSTRING(TO_CHAR(fm.scraped_date, 'MM/YYYY'),1,2)




