{{
    config(
        unique_key='SUBURB_NAME',
        alias='lga_suburb'
    )
}}

select * from {{ source('raw', 'RAW_NSW_LGA_SUBURB') }}