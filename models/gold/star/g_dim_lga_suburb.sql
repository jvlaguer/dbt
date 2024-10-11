{{
    config(
        unique_key='suburb_name',
        alias='dim_suburb_name'
    )
}}

with

source  as (

    select * from {{ ref('lga_suburb_snapshot') }}

),

cleaned as (
    select
        suburb_name,
        lga_name,
        case when dbt_valid_from = (select min(dbt_valid_from) from source) then '1900-01-01'::timestamp else dbt_valid_from end as valid_from,
        dbt_valid_to as valid_to
    from source
),

unknown as (
    select
        'unknown' as suburb_name,
        'unknown' as lga_name,
        '1900-01-01'::timestamp  as valid_from,
        null::timestamp as valid_to

)
select * from unknown
union all
select * from cleaned