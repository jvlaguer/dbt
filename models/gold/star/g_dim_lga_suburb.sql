{{
    config(
        unique_key='suburb_name',
        alias='dim_suburb_name'
    )
}}

WITH

source  AS (

    SELECT * FROM {{ ref('lga_suburb_snapshot') }}

),

cleaned as (
    SELECT
        suburb_name,
        lga_name,
        CASE WHEN dbt_valid_from = (SELECT MIN(dbt_valid_from) FROM source) THEN '1900-01-01'::timestamp ELSE dbt_valid_from END as valid_from,
        dbt_valid_to as valid_to
    FROM source
),

unknown as (
    select
        'unknown' as suburb_name,
        'unknown' as lga_name,
        '1900-01-01'::timestamp  as valid_from,
        null::timestamp as valid_to

)
SELECT * FROM unknown
UNION ALL
SELECT * FROM cleaned