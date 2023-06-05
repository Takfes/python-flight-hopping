

drop table IF EXISTS flights_opt;

CREATE TABLE IF NOT EXISTS flights_opt AS (
    SELECT
    *
    ,("Arrival_date"||' '||"Arrival_time")::timestamp as Arrival_timestamp
    ,("Departure_date"||' '||"Departure_time")::timestamp as Departure_timestamp
    FROM(
        SELECT ROW_NUMBER() OVER (PARTITION BY "Departure_date","Origin_Code","Destination_Code" ORDER BY "Price" ASC) AS RN, *
        FROM public.flights
        ORDER BY "Departure_date","Origin_Code","Destination_Code"
        )OPT
    WHERE OPT.RN =1
);