{% snapshot lga_code_snapshot %}

{{
        config(
          strategy='check',
          unique_key='lga_code',
          check_cols=['LGA_CODE', 'LGA_NAME'],
          alias='lga_code'
        )
    }}

select *
from {{ ref('b_lga_code') }}

{% endsnapshot %}