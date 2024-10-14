{{
    config(
        unique_key='listing_id',
        alias='facts'
    )
}}

select
{{ dbt_utils.generate_surrogate_key(['listing_id', 'scrape_id', 'scraped_date', 'host_id']) }} as airbnb_id
, *
from {{ source('raw', 'raw_airbnb_listings') }}