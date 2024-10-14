{{
    config(
        unique_key='listing_id',
        alias='fact_listing_metrics'
    )
}}

select
	listing_id,
    host_id,
	scraped_date,
    price,
	availability_30,
    number_of_reviews,
    review_scores_rating,
    review_scores_accuracy,
    review_scores_cleanliness,
    review_scores_checkin,
    review_scores_communication,
    review_scores_value
from {{ ref('s_airbnb') }}