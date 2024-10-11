{{
    config(
        unique_key='LGA_CODE',
        alias='lga_code'
    )
}}

select * from {{ source('raw', 'RAW_NSW_LGA_CODE') }}