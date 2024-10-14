{% snapshot lga_suburb_snapshot %}

{{
        config(
          strategy='check',
          unique_key='SUBURB_NAME',
          check_cols=['LGA_NAME', 'SUBURB_NAME'],
          alias='lga_suburb'
        )
    }}

select *
from {{ ref('b_lga_suburb') }}

{% endsnapshot %}