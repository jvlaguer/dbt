{{
    config(
        unique_key='LGA_CODE_2016',
        alias='census_02'
    )
}}

select * from {{ source('raw', 'raw_census_g02') }}