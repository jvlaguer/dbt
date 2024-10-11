{{
    config(
        unique_key='LGA_CODE_2016',
        alias='census_02'
    )
}}

with

source  as (

    select * from {{ ref('b_census_02') }}
),

renamed as (
    select
        REPLACE(LGA_CODE_2016,'LGA','') as lga_code,
        median_age_persons,
        median_mortgage_repay_monthly,
        median_tot_prsnl_inc_weekly,
        median_rent_weekly,
        median_tot_fam_inc_weekly,
        average_num_psns_per_bedroom,
        median_tot_hhd_inc_weekly,
        average_household_size

    from source
)

select * from renamed