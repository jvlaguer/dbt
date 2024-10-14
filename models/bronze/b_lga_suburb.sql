{{
    config(
        unique_key='SUBURB_NAME',
        alias='lga_suburb'
    )
}}

select * from {{ source('raw', 'raw_nsw_lga_suburb') }}