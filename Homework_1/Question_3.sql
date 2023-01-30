/*
Count records

How many taxi trips were totally made on January 15?
Tip: started and finished on 2019-01-15.
Remember that lpep_pickup_datetime and lpep_dropoff_datetime columns are in the format timestamp (date and hour+min+sec) and not in date.
*/
select
	count(*)
from public.yellow_taxi_data_201901
where
	lpep_pickup_datetime >= '20190115'
	and lpep_pickup_datetime < '20190116'