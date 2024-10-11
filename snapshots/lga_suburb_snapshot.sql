{% snapshot lga_suburb_snapshot %}

{{
        config(
          strategy='check',
          unique_key='SUBURB_NAME',
          check_cols=['LGA_NAME', 'SUBURB_NAME'],
          alias='lga_suburb'
        )
    }}

select
    LGA_NAME as lga_name,
    SUBURB_NAME as suburb_name
    -- NOW() as updated_at
from {{ ref('b_lga_suburb') }}

{% endsnapshot %}