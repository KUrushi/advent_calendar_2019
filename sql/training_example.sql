SELECT
    [ifnull(pickup_community_area,0)] AS pickup_community_area,
    [ifnull(fare,0)] AS fare,
    [ifnull(EXTRACT(MONTH FROM trip_start_timestamp),0)] AS trip_start_month,
    [ifnull(EXTRACT(DAY FROM trip_start_timestamp), 0)] AS trip_start_hour,
    [ifnull(EXTRACT(DAY FROM trip_start_timestamp), 0)] AS trip_start_day,
    [format_timestamp('%F', trip_start_timestamp)] AS trip_start_timestamp,
    [ifnull(pickup_latitude,0)] AS pickup_latitude,
    [ifnull(pickup_longitude,0)] AS pickup_longitude,
    [ifnull(dropoff_latitude, 0)] AS dropoff_latitude,
    [ifnull(dropoff_longitude, 0)] AS dropoff_longitude,
    [ifnull(trip_miles, 0)] AS trip_miles,
    [ifnull(pickup_census_tract, 0)] AS pickup_census_tract,
    [ifnull(dropoff_census_tract, 0)] AS dropoff_census_tract,
    [ifnull(payment_type, 'None')] AS payment_type,
    [ifnull(company, 'None')] AS company,
    [ifnull(trip_seconds, 0)] AS trip_seconds,
    [ifnull(dropoff_community_area, 0)] AS dropoff_community_area,
    [ifnull(tips, 0)] AS tips
FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
WHERE mod(abs(farm_fingerprint(unique_key)), 1000) < 20
