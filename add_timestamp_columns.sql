
alter table public.flights_opt
add column Departure_timestamp timestamp,
add column Arrival_timestamp timestamp;

UPDATE public.flights_opt
SET Departure_timestamp = ("Departure_date"||' '||"Departure_time")::timestamp;

UPDATE public.flights_opt
set Arrival_timestamp = ("Arrival_date"||' '||"Arrival_time")::timestamp;

select *
from information_schema.columns
where table_name = 'flights_opt';

-- SELECT Departure_timestamp as ts
-- FROM public.flights_opt
-- WHERE  Departure_timestamp >= '2020-03-04 00:00:01'
-- order by  Departure_timestamp;

-- SELECT ("Departure_date"||' '||"Departure_time")::timestamp as ts
-- FROM public.flights_opt
-- WHERE  ("Departure_date"||' '||"Departure_time") >= '2020-03-03 00:00:01'
-- order by  ("Departure_date"||' '||"Departure_time");
