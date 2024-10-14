{% snapshot lga_airbnb_listing %}

{{
        config(
          strategy='timestamp',
          unique_key='listing_id',
          updated_at='scraped_date',
          alias='lga_airbnb_listing'
        )
    }}

SELECT
listing_id,
host_id,
scrape_id,
scraped_date,
listing_neighbourhood,
property_type,
room_type,
accommodates,
FROM {{ ref('b_airbnb') }}

{% endsnapshot %}