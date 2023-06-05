select
ROUND((hop1."Price" + hop2."Price" + hop3."Price")::NUMERIC,2) as TOTAL_price,
(hop3.Arrival_timestamp-hop1.Departure_timestamp) as away_from_home,
(hop2.Departure_timestamp-hop1.Arrival_timestamp) as stay_1,
(hop3.Departure_timestamp-hop2.Arrival_timestamp) as stay_2,
hop1."Origin_Code"||'->'||hop2."Origin_Code"||'->'||hop3."Origin_Code"||'->'||hop3."Destination_Code" as trip,
'leave '|| hop1."Origin_Code"||' @ '||hop1.Departure_timestamp as fligth_1,
'leave '|| hop2."Origin_Code"||' @ '||hop2.Departure_timestamp as fligth_2,
'leave '|| hop3."Origin_Code"||' @ '||hop3.Departure_timestamp as fligth_3,
 hop1."DeeplinkUrl" as link_1,
 hop2."DeeplinkUrl" as link_2,
 hop3."DeeplinkUrl" as link_3
from
	(
	select "Departure_date","Departure_time","Origin_Code","Destination_Code","Arrival_date","Arrival_time","Price","DeeplinkUrl",Arrival_timestamp,Departure_timestamp
	from public.flights_opt
	where "Origin_Code" = 'ATH'
	and "Destination_Code" in ('EDI','BCN','LIS','OSL','KEF','RKV')
	and "Name_Carrier" <> 'Ryanair'
	and "Name_OperatingCarrier" <> 'Ryanair'
	) hop1
left join
	(
	select "Departure_date","Departure_time","Origin_Code","Destination_Code","Arrival_date","Arrival_time","Price","DeeplinkUrl",Arrival_timestamp,Departure_timestamp
	from public.flights_opt
	where "Destination_Code" in ('EDI','BCN','LIS','OSL','KEF','RKV')
	and "Name_Carrier" <> 'Ryanair'
	and "Name_OperatingCarrier" <> 'Ryanair'
	) hop2
on hop1."Destination_Code" = hop2."Origin_Code"
and EXTRACT('Day' from hop2.Departure_timestamp - hop1.Arrival_timestamp) BETWEEN 2 and 5
left join
	(
	select "Departure_date","Departure_time","Origin_Code","Destination_Code","Arrival_date","Arrival_time","Price","DeeplinkUrl",Arrival_timestamp,Departure_timestamp
	from public.flights_opt
	where "Destination_Code" = 'ATH'
	and "Name_Carrier" <> 'Ryanair'
	and "Name_OperatingCarrier" <> 'Ryanair'
	) hop3
on hop2."Destination_Code" = hop3."Origin_Code"
and EXTRACT('Day' from hop3.Departure_timestamp - hop2.Arrival_timestamp) BETWEEN 2 and 5
where 1=1
and hop3.Departure_timestamp is not null
and hop2."Origin_Code" <> hop3."Destination_Code"
and hop1."DeeplinkUrl" not ilike '%2fryan&'
and hop2."DeeplinkUrl" not ilike '%2fryan&'
and hop3."DeeplinkUrl" not ilike '%2fryan&'
order by ROUND((hop1."Price" + hop2."Price" + hop3."Price")::NUMERIC,2)
;