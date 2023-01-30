/*
The number of passengers

In 2019-01-01 how many trips had 2 and 3 passengers?
*/
select
	sq."Zone"
from (
	select
		zdo."Zone",
		rank() over(order by ytd.tip_amount desc) as "rn"
	from public.yellow_taxi_data_201901 as ytd
	join public.zones as zpu on zpu."LocationID" = ytd."PULocationID"
	join public.zones as zdo on zdo."LocationID" = ytd."DOLocationID"
	where
		zpu."Zone" = 'Astoria'
) as sq
where
	sq.rn = 1