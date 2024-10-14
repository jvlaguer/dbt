{% snapshot lga_airbnb_host %}

{{
        config(
          strategy='timestamp',
          unique_key='host_id',
          updated_at='scraped_date',
          alias='lga_airbnb_host'
        )
    }}

SELECT DISTINCT
scraped_date,
host_id,
host_name,
host_since,
host_is_superhost,
host_neighbourhood
FROM {{ ref('b_airbnb') }}

{% endsnapshot %}