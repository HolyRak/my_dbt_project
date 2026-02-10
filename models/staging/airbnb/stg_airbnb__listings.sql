with 

source as (

    select * from {{ source('airbnb', 'listings') }}

),

renamed as (

    select
        id,
        name,
        host_id,
        host_name,
        host_since,
        host_response_time,
        host_response_rate,
        host_is_superhost,
        neighbourhood_cleansed,
        neighbourhood_group_cleansed,
        latitude,
        longitude,
        property_type,
        room_type,
        accommodates,
        bathrooms,
        bedrooms,
        beds,
        price,
        minimum_nights,
        availability_365,
        number_of_reviews,
        review_scores_rating,
        license,
        instant_bookable

    from source

)

select * from renamed