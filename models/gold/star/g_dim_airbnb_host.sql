{{
    config(
        unique_key='host_id',
        alias='dim_airbnb_host'
    )
}}

with

source  as (

    SELECT * FROM {{ ref('lga_airbnb_host') }}

),

cleaned as (
    SELECT
    scrape_id,
    scraped_date,
    host_id,
    host_name,
    host_since,
    host_is_superhost,
    host_neighbourhood,
    case when dbt_valid_from = (select min(dbt_valid_from) from source) then '1900-01-01'::timestamp else dbt_valid_from::timestamp end as valid_from,
    dbt_valid_to as valid_to
    FROM source
),

unknown as (
    select
        0 as scrape_id,
        null::timestamp as scraped_date,
        0 as host_id,
        'unknown' as host_name,
        null::timestamp as host_since,
        'unknown' as host_is_superhost,
        'unknown' as host_neighbourhood,
        '1900-01-01'::timestamp  as valid_from,
        null::timestamp as valid_to

)
SELECT * FROM unknown
UNION ALL
SELECT * FROM cleaned