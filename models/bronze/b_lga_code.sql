{{
    config(
        unique_key='LGA_CODE',
        alias='lga_code'
    )
}}

select * from {{ source('raw', 'raw_nsw_lga_code') }}