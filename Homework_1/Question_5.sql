/*
The number of passengers

In 2019-01-01 how many trips had 2 and 3 passengers?
*/
select
	Passenger_count,
	count(*)
from public.yellow_taxi_data_201901
where
	lpep_pickup_datetime >= '20190101'
	and lpep_pickup_datetime < '20190102'
	and Passenger_count in (2,3)
group by
	Passenger_count