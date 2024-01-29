-- Q : How many taxi trips were totally made on September 18th 2019?

SELECT COUNT(*) AS total_trips
FROM public.tripdata AS td
WHERE td.lpep_pickup_datetime::date = '2019-09-18' 
	AND td.lpep_dropoff_datetime::date = '2019-09-18';

-- Q : Which was the pick up day with the largest trip distance. Use the pick up time for your calculations.

SELECT td.lpep_pickup_datetime::date AS date_trip,
     MAX(trip_distance) AS largest_distance
FROM public.tripdata AS td
GROUP BY td.lpep_pickup_datetime::date
ORDER BY largest_distance DESC
LIMIT 1;

-- Q : Consider lpep_pickup_datetime in '2019-09-18' and ignoring Borough has Unknown
-- Which were the 3 pick up Boroughs that had a sum of total_amount superior to 50000?

SELECT tz."Borough",
	SUM(td.total_amount) AS total_amount
FROM public.tripdata AS td
INNER JOIN public.taxizone AS tz
    ON td."PULocationID" = tz."LocationID"
WHERE td.lpep_pickup_datetime::date = '2019-09-18'
    AND tz."Borough" <> 'Unknown'
GROUP BY tz."Borough"
HAVING SUM(td.total_amount) > 50000;


-- Q : For the passengers picked up in September 2019 in the zone name Astoria which was the drop off zone that had the largest tip?
-- We want the name of the zone, not the id.

SELECT tzdo."Zone",
	td.tip_amount AS tip_amount
FROM public.tripdata AS td
INNER JOIN public.taxizone AS tzpu
ON td."PULocationID" = tzpu."LocationID"
INNER JOIN public.taxizone AS tzdo
ON td."DOLocationID" = tzdo."LocationID"
WHERE td.lpep_pickup_datetime::date BETWEEN '2019-09-01' AND '2019-09-30'
AND tzpu."Zone" = 'Astoria'
ORDER BY td.tip_amount DESC
LIMIT 1;