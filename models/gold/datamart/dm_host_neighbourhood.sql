{{
    config(
        alias='dm_host_neighbourhood'
    )
}}

SELECT
s.lga_name as host_neighbourhood_lga,
fm.TO_CHAR(scraped_date, 'MMM/YYYY') as month_year,
COUNT(DISTINCT h.host_id) as cnt_distinct_host,
SUM(IF(fm.has_availability='t',(30-fm.availability_30)*fm.price,NULL)) as est_rev_per_active_listing,
SUM(IF(fm.has_availability='t',(30-fm.availability_30)*fm.price,NULL))/COUNT(DISTINCT host_id) as est_rev_per_active_listing_per_host
from {{ ref('g_fact_listing_metrics') }} fm
LEFT JOIN {{ ref('g_dim_airbnb_host') }} h on fm.host_id = s.host_id and h.date::timestamp >= h.valid_from and fm.date::timestamp < coalesce(h.valid_to, '9999-12-31'::timestamp)
LEFT JOIN {{ ref('g_dim_lga_suburb') }} s  on fm.host_neighborhood = s.suburb_name and fm.date::timestamp >= s.valid_from and fm.date::timestamp < coalesce(s.valid_to, '9999-12-31'::timestamp)
GROUP BY 1,2
ORDER BY 1,2;




