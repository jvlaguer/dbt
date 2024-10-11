{% snapshot lga_code_snapshot %}

{{
        config(
          strategy='check',
          unique_key='lga_code',
          check_cols=['LGA_CODE', 'LGA_NAME'],
          alias='lga_code'
        )
    }}

select
    LGA_CODE as lga_code,
    LGA_NAME as lga_name,
    NOW() as updated_at
from {{ ref('b_lga_code') }}

{% endsnapshot %}