
/* query to take data to add in neo4j*/


SELECT
Departure_date
 ,Origin_Code
 ,Destination_Code
 ,Name_Carrier
 ,Name_OperatingCarrier
 ,CAST(FlightNumber as VARCHAR) AS FlightNumber
 ,Departure_date
 ,Departure_time
 ,Arrival_date
 ,Arrival_time
 ,DeeplinkUrl
 ,MIN(Price) as MINPRICE
 ,ROUND(STDEV(Price),3) as STDEVPRICE
 ,ROUND(STDEV(Price)/AVG(Price),3) as stdev_on_mean
 ,COUNT(*) as CNT
 FROM raw_flights
 WHERE 1=1
 AND Origin_Code IN ("CDG", "MAD", "FCO", "AMS", "ATH")
 AND Destination_Code IN ("CDG", "MAD", "FCO", "AMS", "ATH")
 AND Departure_time BETWEEN "08:00:00" AND "22:00:00"
 GROUP BY Departure_date, Origin_Code, Destination_Code
 ORDER BY Departure_date, Origin_Code, Destination_Code, Departure_time;


SELECT COUNT(*) FROM (
SELECT
Departure_date
 ,Origin_Code
 ,Destination_Code
 ,Name_Carrier
 ,Name_OperatingCarrier
 ,CAST(FlightNumber as VARCHAR) AS FlightNumber
 ,Departure_date
 ,Departure_time
 ,Arrival_date
 ,Arrival_time
 ,DeeplinkUrl
 ,MIN(Price) as Min_Price
 ,ROUND(STDEV(Price),3) as Stdev
 ,count(*) as CNT
 FROM raw_flights
 WHERE 1=1
 AND Origin_Code IN ("CDG", "MAD", "FCO", "AMS", "ATH")
 AND Destination_Code IN ("CDG", "MAD", "FCO", "AMS", "ATH")
 AND Departure_time BETWEEN "08:00:00" AND "22:00:00"
 GROUP BY Departure_date, Origin_Code, Destination_Code
 ORDER BY Departure_date, Origin_Code, Destination_Code, Departure_time
)A;


-- NEEDS FIXING
--SELECT
--	Departure_date
--	,Origin_Code
--	,Destination_Code
--	,Name_Carrier
--	,Name_OperatingCarrier
--	,CAST(FlightNumber as VARCHAR) AS FlightNumber
--	,Departure_date
--	,Departure_time
--	,Arrival_date
--	,Arrival_time
--	,DeeplinkUrl
--	,Price
--	,MIN(Price) as Min_Price
--	,ROUND(STDEV(Price),3) as Stdev
--	,count(*) as CNT
--FROM (
--		 SELECT *, ROW_NUMBER() OVER (PARTITION BY Departure_date, Origin_Code, Destination_Code ORDER BY Price) AS RN
--		 FROM raw_flights
--		 WHERE 1=1
--		 AND Origin_Code IN ("CDG", "MAD", "FCO", "AMS", "ATH")
--		 AND Destination_Code IN ("CDG", "MAD", "FCO", "AMS", "ATH")
--		 AND Departure_time BETWEEN "08:00:00" AND "22:00:00"
--		) nested
--WHERE nested.RN = 1
-- --GROUP BY Departure_date, Origin_Code, Destination_Code
-- --ORDER BY Departure_date, Origin_Code, Destination_Code, Departure_time




/*---------------------------------------------------------------------------------------------*/

SELECT * FROM(
SELECT *, ROW_NUMBER() OVER (PARTITION BY Departure_Date_f1 ORDER BY Departure_Date_f1 ASC) AS RN 
FROM result_df_2
WHERE 1=1
AND ABS(Hop_1_duration - Hop_2_duration) < ABS(Hop_2_duration + Hop_1_duration)/2
AND Total_duration BETWEEN 5 AND 12 
)A
WHERE A.RN=1;


-- add price per day field
SELECT Departure_Date_f1,total_list,* FROM
(SELECT *
,ROW_NUMBER() OVER (PARTITION BY Departure_Date_f1 ORDER BY Departure_Date_f1 ASC) AS RN 
FROM result_df_2
WHERE 1=1
AND ABS(Hop_1_duration - Hop_2_duration) < ABS(Hop_2_duration + Hop_1_duration)/2
AND Total_duration BETWEEN 5 AND 12)AS A
WHERE A.RN=1
ORDER BY 1;


-----------------------------------------------------
---------------- sugg_day_lvl -----------------------
-----------------------------------------------------
/*cheapest flight per day*/
/*maybe provide one level more granular results, meaning the table before applying the A.RN=1 filter */

DROP TABLE IF EXISTS sugg_day_lvl ;

CREATE TABLE sugg_day_lvl AS
SELECT Departure_Date_f1,total_list FROM
(SELECT *
,ROW_NUMBER() OVER (PARTITION BY Departure_Date_f1 ORDER BY total_list ASC) AS RN 
FROM result_df_2
WHERE 1=1
AND ABS(Hop_1_duration - Hop_2_duration) < ABS(Hop_2_duration + Hop_1_duration)/2
AND Total_duration BETWEEN 5 AND 12)AS A
WHERE A.RN=1
ORDER BY 1;

SELECT * FROM sugg_day_lvl;

-----------------------------------------------------
---------------- sugg_day_hopcombo_lvl --------------
-----------------------------------------------------
/*cheapest flight per hops_combo per day*/
/*maybe provide one level more granular results, meaning the table before applying the A.RN=1 filter */

DROP TABLE IF EXISTS sugg_day_hopcombo_lvl ;

CREATE TABLE sugg_day_hopcombo_lvl AS
SELECT Departure_Date_f1,airportcode_list_h1,airportcode_list_h2,airportcode_list_end,total_list FROM
(SELECT *
,ROW_NUMBER() OVER (PARTITION BY Departure_Date_f1,airportcode_list_h1,airportcode_list_h2,airportcode_list_end ORDER BY total_list ASC) AS RN 
FROM result_df_2
WHERE 1=1
AND ABS(Hop_1_duration - Hop_2_duration) < ABS(Hop_2_duration + Hop_1_duration)/2
AND Total_duration BETWEEN 5 AND 12)AS A
WHERE A.RN=1
ORDER BY 1;

SELECT * FROM sugg_day_hopcombo_lvl;


/* JOIN COMBOS WITH MAP */

CREATE TABLE combos_map AS
SELECT a.*
,b.Code AS s0Code,  b.AirportName AS s0AirportName,  b.City AS s0City,  b.Country AS s0Country,  b.lat AS s0lat,  b.lon AS s0lon
,c.Code AS s1Code,  c.AirportName AS s1AirportName,  c.City AS s1City,  c.Country AS s1Country,  c.lat AS s1lat,  c.lon AS s1lon
,d.Code AS s2Code,  d.AirportName AS s2AirportName,  d.City AS s2City,  d.Country AS s2Country,  d.lat AS s2lat,  d.lon AS s2lon
,e.Code AS s3Code,  e.AirportName AS s3AirportName,  e.City AS s3City,  e.Country AS s3Country,  e.lat AS s3lat,  e.lon AS s3lon
FROM combos a
LEFT JOIN (SELECT Code,AirportName,City,Country,lat,lon FROM airports) b ON a.s0 = b.Code
LEFT JOIN (SELECT Code,AirportName,City,Country,lat,lon FROM airports) c ON a.s1 = c.Code
LEFT JOIN (SELECT Code,AirportName,City,Country,lat,lon FROM airports) d ON a.s2 = d.Code
LEFT JOIN (SELECT Code,AirportName,City,Country,lat,lon FROM airports) e ON a.s3 = e.Code;


/* */

-- Create sorted tables with suggestions on day level
DROP TABLE IF EXISTS sug_date;

CREATE TABLE sug_date AS
SELECT *
FROM(
			SELECT * ,ROW_NUMBER() OVER (PARTITION BY d1date ORDER BY tp ASC) AS RN_date 
			FROM combos_map
			WHERE 1=1
			AND ABS(h1d - h2d) < td/2
			AND td BETWEEN 5 AND 12
			);
		
SELECT COUNT(*) FROM sug_date;
		
-- Create sorted tables with suggestions on day&combo level
DROP TABLE sug_cdate IF EXISTS;
CREATE TABLE sug_cdate AS
SELECT *
FROM(
			SELECT * ,ROW_NUMBER() OVER (PARTITION BY d1date,s1,s2 ORDER BY tp ASC) AS RN_cdate 
			FROM sug_date
			WHERE 1=1
			AND ABS(h1d - h2d) < td/2
			AND td BETWEEN 5 AND 12
			);

SELECT COUNT(*) FROM sug_cdate;
SELECT * FROM sug_cdate LIMIT 10;



