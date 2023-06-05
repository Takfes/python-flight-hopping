
MATCH (o:Airport {airportCode:"ATH"})-[:FLYING_FROM]->(f:Flight)-[:FLYING_TO]->(d:Airport) WHERE d.airportCode IN ["MAD","AMS"] RETURN f.departure,f.price,d.airportName ORDER BY f.price ASC, f.departure ASC

MATCH (o:Airport {airportCode:"ATH"})-[:FLYING_FROM]->(f:Flight)-[:FLYING_TO]->(s1:Airport) WHERE d.airportCode IN ["MAD","AMS"]
MATCH (o:Airport {airportCode:"ATH"})-[:FLYING_FROM]->(f:Flight)-[:FLYING_TO]->(s1:Airport) WHERE NOT s1.airportCode IN [o.airportCode] RETURN *
MATCH (o:Airport {airportCode:"ATH"})-[:FLYING_FROM]->(f:Flight)-[:FLYING_TO]->(s1) WHERE NOT s1.airportCode IN [o.airportCode] RETURN s1

MATCH (o:Airport {airportCode:"ATH"})-[:FLYING_FROM]->(f1:Flight)-[:FLYING_TO]->(s1:Airport) WHERE NOT s1.airportCode IN [o.airportCode]
WITH o, s1, f1
MATCH (s1)-[:FLYING_FROM]->(f2:Flight)-[:FLYING_TO]->(s2:Airport) WHERE NOT s2.airportCode IN [o.airportCode,s1.airportCode] AND f1.departure<f2.departure RETURN *

MATCH (o:Airport {airportCode:"ATH"})-[:FLYING_FROM]->(f1:Flight)-[:FLYING_TO]->(s1:Airport) WHERE NOT s1.airportCode IN [o.airportCode]
WITH o, s1, f1
MATCH (s1)-[:FLYING_FROM]->(f2:Flight)-[:FLYING_TO]->(s2:Airport) WHERE NOT s2.airportCode IN [o.airportCode,s1.airportCode] AND f1.arrival<f2.departure 
RETURN o.airportCode, f1.departure, f1.price, s1.airportCode, f2.departure, f2.price, s2.airportCode, f1.price+f2.price as total  
ORDER BY total ASC
//ORDER BY f1.departure ASC, f2.departure ASC

MATCH (o:Airport {airportCode:"ATH"})-[:FLYING_FROM]->(f1:Flight)-[:FLYING_TO]->(s1:Airport) WHERE NOT s1.airportCode IN [o.airportCode]
WITH o, s1, f1
MATCH (s1)-[:FLYING_FROM]->(f2:Flight)-[:FLYING_TO]->(s2:Airport) WHERE NOT s2.airportCode IN [o.airportCode,s1.airportCode] AND f1.arrival<f2.departure 
WITH o, s1, s2, f1, f2
MATCH (s2)-[:FLYING_FROM]->(f3:Flight)-[:FLYING_TO]->(s3:Airport) WHERE NOT s3.airportCode IN [o.airportCode,s1.airportCode,s2.airportCode] AND f2.arrival<f3.departure 
RETURN *

MATCH (o:Airport {airportCode:"ATH"})-[:FLYING_FROM]->(f1:Flight)-[:FLYING_TO]->(s1:Airport) WHERE NOT s1.airportCode IN [o.airportCode]
WITH o, s1, f1
MATCH (s1)-[:FLYING_FROM]->(f2:Flight)-[:FLYING_TO]->(s2:Airport) WHERE NOT s2.airportCode IN [o.airportCode,s1.airportCode] AND f1.arrival<f2.departure 
WITH o, s1, s2, f1, f2
MATCH (s2)-[:FLYING_FROM]->(f3:Flight)-[:FLYING_TO]->(s3:Airport) WHERE NOT s3.airportCode IN [o.airportCode,s1.airportCode,s2.airportCode] AND f2.arrival<f3.departure 
//RETURN *
RETURN o.airportCode, f1.departure, f1.price, s1.airportCode, 
					  f2.departure, f2.price, s2.airportCode, 
					  f3.departure, f3.price, s3.airportCode, 
					  f1.price+f2.price+f3.price as total  
ORDER BY total ASC


MATCH (o:Airport {airportCode:"ATH"})-[:FLYING_FROM]->(f1:Flight)-[:FLYING_TO]->(s1:Airport) WHERE NOT s1.airportCode IN [o.airportCode]
WITH o, s1, f1
MATCH (s1)-[:FLYING_FROM]->(f2:Flight)-[:FLYING_TO]->(s2:Airport) WHERE NOT s2.airportCode IN [o.airportCode,s1.airportCode] AND f1.arrival<f2.departure 
WITH o, s1, s2, f1, f2
MATCH (s2)-[:FLYING_FROM]->(f3:Flight)-[:FLYING_TO]->(s3:Airport) WHERE NOT s3.airportCode IN [o.airportCode,s1.airportCode,s2.airportCode] AND f2.arrival<f3.departure 
RETURN MIN(f1.price+f2.price+f3.price) as total, departuredate
ORDER BY total ASC



MATCH (o:Airport {airportCode:"LHR"})-[:FLYING_FROM]->(f1:Flight)-[:FLYING_TO]->(s1:Airport) WHERE NOT s1.airportCode IN [o.airportCode]
WITH o, s1, f1
MATCH (s1)-[:FLYING_FROM]->(f2:Flight)-[:FLYING_TO]->(s2:Airport) WHERE NOT s2.airportCode IN [o.airportCode,s1.airportCode] AND f1.arrival<f2.departure 
WITH o, s1, s2, f1, f2
MATCH (s2)-[:FLYING_FROM]->(f3:Flight)-[:FLYING_TO]->(s3:Airport) WHERE NOT s3.airportCode IN [o.airportCode,s1.airportCode,s2.airportCode] AND f2.arrival<f3.departure 
WITH o, s1, s2, f1, f2, s3, f3 
MATCH (s3)-[:FLYING_FROM]->(f4:Flight)-[:FLYING_TO]->(s4:Airport) WHERE s4.airportCode IN [o.airportCode] AND f3.arrival<f4.departure 
RETURN 
	o.airportCode   AS s0
	,f1.departure   AS d1
	,f1.price       AS p1
	,f1.url         AS u1
	
	,s1.airportCode AS s1
	,f2.departure   AS d2
	,f2.price       AS p2
	,f2.url         AS u2
	
	,s2.airportCode AS s3
	,f3.departure   AS d4
	,f3.price       AS p4
	,f3.url         AS u4
	
	,s3.airportCode AS s4
	,f4.departure   AS d5
	,f4.price       AS p5
	,f3.url         AS u5
	
	,s4.airportCode AS s5
	
	,f1.price+f2.price+f3.price+f4.price as total 
ORDER BY total ASC




MATCH (o:Airport {airportCode:"LHR"})-[:FLYING_FROM]->(f1:Flight)-[:FLYING_TO]->(s1:Airport) WHERE NOT s1.airportCode IN [o.airportCode]
WITH o, s1, f1
MATCH (s1)-[:FLYING_FROM]->(f2:Flight)-[:FLYING_TO]->(s2:Airport) WHERE NOT s2.airportCode IN [o.airportCode,s1.airportCode] AND f1.arrival<f2.departure 
WITH o, s1, s2, f1, f2
MATCH (s2)-[:FLYING_FROM]->(f3:Flight)-[:FLYING_TO]->(s3:Airport) WHERE NOT s3.airportCode IN [o.airportCode,s1.airportCode,s2.airportCode] AND f2.arrival<f3.departure 
WITH o, s1, s2, f1, f2, s3, f3 
MATCH (s3)-[:FLYING_FROM]->(f4:Flight)-[:FLYING_TO]->(s4:Airport {airportCode:"LHR"}) WHERE f3.arrival<f4.departure 
RETURN 
	o.airportCode   AS s0
	,f1.departure   AS d1
	,f1.price       AS p1
	,f1.url         AS u1
	
	,s1.airportCode AS s1
	,f2.departure   AS d2
	,f2.price       AS p2
	,f2.url         AS u2
	
	,s2.airportCode AS s3
	,f3.departure   AS d3
	,f3.price       AS p3
	,f3.url         AS u3
	
	,s3.airportCode AS s3
	,f4.departure   AS d4
	,f4.price       AS p4
	,f3.url         AS u4
	
	,s4.airportCode AS s4
	
	,f1.price+f2.price+f3.price+f4.price as total 
ORDER BY total ASC


   
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////  THIS WORKS FOR 2 HOPS /////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

 MATCH (s0:Airport {airportCode:"FCO"})-[:FLYING_FROM]->(f1:Flight)-[:FLYING_TO]->(s1:Airport) WHERE NOT s1.airportCode IN [s0.airportCode]
            WITH s0, s1, f1
            MATCH (s1:Airport)-[:FLYING_FROM]->(f2:Flight)-[:FLYING_TO]->(s2:Airport) WHERE NOT s2.airportCode IN [s0.airportCode,s1.airportCode] AND f1.arrival<f2.departure 
            WITH s0, s1, s2, f1, f2                                                                         
            MATCH (s3:Airport {airportCode:"FCO"})<-[:FLYING_TO]-(f3:Flight)<-[:FLYING_FROM]-(s2:Airport) WHERE f2.arrival<f3.departure
            RETURN 
            	s0.airportCode  AS s0
            	,f1.departure   AS d1
            	,f1.price       AS p1
            	,f1.url         AS u1
            	
            	,s1.airportCode AS s1
            	,f2.departure   AS d2
            	,f2.price       AS p2
            	,f2.url         AS u2
            	
            	,s2.airportCode AS s2
            	,f3.departure   AS d3
            	,f3.price       AS p3
            	,f3.url         AS u3
            
            	,s3.airportCode AS s3
            	
            	,f1.price+f2.price+f3.price as total 
            ORDER BY total ASC
			
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////  THIS IS THE SAME WITH THE ABOVE WITH DIFFERENT DIRECTION IN THE LAST RELATIONSHIP /////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

 MATCH (s0:Airport {airportCode:"FCO"})-[:FLYING_FROM]->(f1:Flight)-[:FLYING_TO]->(s1:Airport) WHERE NOT s1.airportCode IN [s0.airportCode]
            WITH s0, s1, f1
            MATCH (s1:Airport)-[:FLYING_FROM]->(f2:Flight)-[:FLYING_TO]->(s2:Airport) WHERE NOT s2.airportCode IN [s0.airportCode,s1.airportCode] AND f1.arrival<f2.departure 
            WITH s0, s1, s2, f1, f2                                                                         
            MATCH (s2:Airport)-[:FLYING_FROM]->(f3:Flight)-[:FLYING_TO]->(s3:Airport {airportCode:"FCO"}) WHERE f2.arrival<f3.departure
            RETURN 
            	s0.airportCode  AS s0
            	,f1.departure   AS d1
            	,f1.price       AS p1
            	,f1.url         AS u1
            	
            	,s1.airportCode AS s1
            	,f2.departure   AS d2
            	,f2.price       AS p2
            	,f2.url         AS u2
            	
            	,s2.airportCode AS s2
            	,f3.departure   AS d3
            	,f3.price       AS p3
            	,f3.url         AS u3
            
            	,s3.airportCode AS s3
            	
            	,f1.price+f2.price+f3.price as total 
            ORDER BY total ASC 
			
			
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////  THIS IS AN ATTEMPT FOR 3 HOPS + ORIGIN/DESTINATION ROUND TRIP ; 4 FLIGHTS /////////////////////////////////////////////////////////
////////////////////////////////// RUNS OUT OF MEMORY !!!!!! ///////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

          MATCH (s0:Airport {airportCode:"FCO"})-[:FLYING_FROM]->(f1:Flight)-[:FLYING_TO]->(s1:Airport) WHERE NOT s1.airportCode IN [s0.airportCode]
            WITH s0, s1, f1
            MATCH (s1:Airport)-[:FLYING_FROM]->(f2:Flight)-[:FLYING_TO]->(s2:Airport) WHERE NOT s2.airportCode IN [s0.airportCode,s1.airportCode] AND f1.arrival<f2.departure 
            WITH s0, s1, s2, f1, f2                                                                         
            MATCH (s2:Airport)-[:FLYING_FROM]->(f3:Flight)-[:FLYING_TO]->(s3:Airport) WHERE NOT s3.airportCode IN [s0.airportCode,s1.airportCode,s2.airportCode] AND f2.arrival<f3.departure 
			WITH s0, s1, s2, s3, f1, f2, f3                                                                         
            MATCH (s3:Airport)-[:FLYING_FROM]->(f4:Flight)-[:FLYING_TO]->(s4:Airport {airportCode:"FCO"}) WHERE f3.arrival<f4.departure
            RETURN 
            	 s0.airportCode  AS s0
            	,f1.departure   AS d1
            	,f1.price       AS p1
            	,f1.url         AS u1
            	
            	,s1.airportCode AS s1
            	,f2.departure   AS d2
            	,f2.price       AS p2
            	,f2.url         AS u2
            	
            	,s2.airportCode AS s2
            	,f3.departure   AS d3
            	,f3.price       AS p3
            	,f3.url         AS u3
            
            	,s3.airportCode AS s3
            	,f4.departure   AS d4
				,f4.price       AS p4
				,f4.url         AS u4
				
				,s4.airportCode AS s4
				
            	,f1.price+f2.price+f3.price+f4.price as total 
            ORDER BY total ASC 
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			
			