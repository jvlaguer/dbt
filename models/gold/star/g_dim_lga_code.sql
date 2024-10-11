{{
    config(
        unique_key='lga_code',
        alias='dim_lga_code'
    )
}}

with

source  as (

    select * from {{ ref('lga_code_snapshot') }}

),

cleaned as (
    select
        lga_code,
        lga_name,
        case when dbt_valid_from = (select min(dbt_valid_from) from source) then '1900-01-01'::timestamp else dbt_valid_from end as valid_from,
        dbt_valid_to as valid_to
    from source
),

unknown as (
    select
        0 as lga_code,
        'unknown' as lga_name,
        '1900-01-01'::timestamp  as valid_from,
        null::timestamp as valid_to

)
select * from unknown
union all
select * from cleaned