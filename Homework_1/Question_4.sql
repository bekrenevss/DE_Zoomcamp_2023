/*
Largest trip for each day

Which was the day with the largest trip distance?
*/
select
	sq.lpep_pickup_date
from (
	select
		cast(lpep_pickup_datetime as date) as "lpep_pickup_date",
		rank() over(order by trip_distance desc) as "rn" -- If we have a few days with the same "trip_distance"
	from public.yellow_taxi_data_201901
) as sq
where
	sq.rn = 1