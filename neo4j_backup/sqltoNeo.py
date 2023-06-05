
# NEO4j
# uri='bolt://localhost:7687'

from py2neo import Graph, Database, Node, Relationship
import os, time, sqlite3
import pandas as pd
import numpy as np

os.chdir("C:\\Users\\takis\\Desktop\\Dockerneo4j")
os.getcwd()

graph = Graph(password="takis")
graph.delete_all()


'''
QUERY RDB ACCORDING TO INITIAL USER PREFERENCES TO OBTAIN DATA TO IMPORT IN NEO4J
'''

db = sqlite3.connect('airflights.db')
USER_DESTINATIONS   = ('CDG','MAD','FCO','AMS', 'ATH')
DEPARTURE_TIME_FROM = '"08:00:00"'
DEPARTURE_TIME_TO   = '"22:00:00"'
TABLE_NAME          = 'raw_flights' 

user_query = """SELECT
                 Departure
                 ,Arrival
                 ,Origin_Code
                 ,Origin_Name
                 ,Destination_Code
                 ,Destination_Name
                 ,Name_Carrier
                 ,Name_OperatingCarrier
                 ,CAST(FlightNumber as VARCHAR) AS FlightNumber
                 ,Departure_date
                 ,Departure_time
                 ,Arrival_date
                 ,Arrival_time
                 ,DeeplinkUrl
                 ,MIN(Price) as Price
                 --,STDEV(Price)
                 ,count(*) as CNT
                 ,QuoteAgeInMinutes
                 FROM {}
                 WHERE 1=1
                 AND Origin_Code IN {}
                 AND Destination_Code IN {}
                 AND Departure_time BETWEEN {} AND {}
                 GROUP BY Departure_date, Origin_Code, Destination_Code
                 ORDER BY Departure_date, Origin_Code, Destination_Code, Departure_time;
                """.format(TABLE_NAME,USER_DESTINATIONS,USER_DESTINATIONS,DEPARTURE_TIME_FROM,DEPARTURE_TIME_TO)


#import pprint
#pp = pprint.PrettyPrinter(indent=4)
#pp.pprint(user_query)

df = pd.read_sql_query(user_query,db)
df.shape
#db.close()


'''
FUNCTION TO IMPORT RDB QUERY RESULTS IN NEO4j
'''

# Create the graph in Neo4j
def pandas_to_neo(dataframe,graph):

    query = ''' 
    		MERGE (orig:Airport {airportName: {origin_name},
    							 airportCode: {origin_code}})

    		MERGE (dest:Airport {airportName: {destination_name},
    							 airportCode: {destination_code}})

    		MERGE (flight:Flight {flight:  {flight},
    							 arrival: {arrival},
    							 departure: {departure},
                                 departuredate: {departuredate},
    							 price: {price},
    							 carrier_name: {carrier_name},
    							 operating_carrier_name: {operating_carrier_name},
    							 url: {link},
    							 recency: {recency}
    							})
    		WITH orig, dest, flight
    		CREATE (orig)-[:FLYING_FROM {departure:{departure}}]->(flight)-[:FLYING_TO {arrival:{arrival}}]->(dest)
    		'''

    for index, row in dataframe.iterrows():
        parameters = {
                      'link'                  : row['DeeplinkUrl']
                    , 'price'                 : row['Price']
                    , 'recency'               : row['QuoteAgeInMinutes']
                    , 'arrival'               : row['Arrival']
                    , 'departure'             : row['Departure']
                    , 'departuredate'         : row['Departure_date']
                    , 'flight'                : row['FlightNumber']
                    , 'carrier_name'          : row['Name_Carrier']
                    , 'operating_carrier_name': row['Name_OperatingCarrier']
                    , 'origin_code'           : row['Origin_Code']
                    , 'origin_name'           : row['Origin_Name']
                    , 'destination_code'      : row['Destination_Code']
                    , 'destination_name'      : row['Destination_Name']
                            }
        graph.run(query, parameters)


#graph.delete_all()
        
start = time.time()
pandas_to_neo(df,graph)
end = time.time()
print(f'Operation took {round(end-start,2)} seconds to complete')


'''
QUERY TO MATCH PATTERN AND DERIVE 4 HOPS ROUND FLIGHTS ARRIVING @ THE ORIGIN STATION
'''

# query Neo4j
neo_query = '''
            MATCH (s0:Airport {airportCode:"ATH"})-[:FLYING_FROM]->(f1:Flight)-[:FLYING_TO]->(s1:Airport) WHERE NOT s1.airportCode IN [s0.airportCode]
            WITH s0, s1, f1
            MATCH (s1:Airport)-[:FLYING_FROM]->(f2:Flight)-[:FLYING_TO]->(s2:Airport) WHERE NOT s2.airportCode IN [s0.airportCode,s1.airportCode] AND f1.arrival<f2.departure 
            WITH s0, s1, s2, f1, f2                                                                         
            MATCH (s2:Airport)-[:FLYING_FROM]->(f3:Flight)-[:FLYING_TO]->(s3:Airport {airportCode:"ATH"}) WHERE f2.arrival<f3.departure
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
            '''

            
start = time.time()
neo_query_result = graph.run(neo_query)
neo_results = neo_query_result.to_table()
end = time.time()
print(f'Operation took {round(end-start,2)} seconds to complete')


#graph.delete_all()

len(neo_results)
type(neo_results)
neo_results[10]
len(neo_results[10])
neo_results

'''
PARSE NEO QUERY RESULTS INTO A DATAFRAME
'''

# Function to parse 14 elements - 2 hops configuration
# instantiate lists to hold the results
def neo_to_pandas(neo_results):
    s0 = []
    d1 = []
    p1 = []
    u1 = []
    s1 = []
    d2 = []
    p2 = []
    u2 = []
    s2 = []
    d3 = []
    p3 = []
    u3 = []
    s3 = []
    tp = []
    
    for x in neo_results:
        s0.append(x[0])
        d1.append(x[1])
        p1.append(x[2])
        u1.append(x[3])
        s1.append(x[4])
        d2.append(x[5])
        p2.append(x[6])
        u2.append(x[7])
        s2.append(x[8])
        d3.append(x[9])
        p3.append(x[10])
        u3.append(x[11])
        s3.append(x[12])
        tp.append(x[13])
    
    result_dict = {
    				's0' : s0
    				,'d1' : d1
    				,'p1' : p1
    				,'u1' : u1
    				,'s1' : s1
    				,'d2' : d2
    				,'p2' : p2
    				,'u2' : u2
    				,'s2' : s2
    				,'d3' : d3
    				,'p3' : p3
    				,'u3' : u3
    				,'s3' : s3
    				,'tp' : tp
    				}

    return pd.DataFrame(result_dict)


combos = neo_to_pandas(neo_results)
combos.shape
'''
ADD FEATURES IN THE DF
'''

combos.shape
combos['d1date'] = pd.to_datetime(combos.d1.apply(lambda x:x[:10]))
combos['d2date'] = pd.to_datetime(combos.d2.apply(lambda x:x[:10]))
combos['d3date'] = pd.to_datetime(combos.d3.apply(lambda x:x[:10]))

combos['h1d'] = (combos['d2date'] - combos['d1date'])/ np.timedelta64(1, 'D')
combos['h2d'] = (combos['d3date'] - combos['d2date'])/ np.timedelta64(1, 'D')
combos['td'] = (combos['h1d'] + combos['h2d'])

combos.shape
combos.head().to_clipboard()

combos.to_sql("combos",db,if_exists='replace',index=True)
combos.to_csv("combos_ATH.csv")



# sqlite query
sqlite_cheapest_per_day = """
                        SELECT Departure_Date_f1,total_list,* FROM
                        (SELECT *
                        ,ROW_NUMBER() OVER (PARTITION BY Departure_Date_f1 ORDER BY Departure_Date_f1 ASC) AS RN 
                        FROM result_df_2
                        WHERE 1=1
                        AND ABS(Hop_1_duration - Hop_2_duration) < ABS(Hop_2_duration + Hop_1_duration)/2
                        AND Total_duration BETWEEN 5 AND 12)AS A
                        WHERE A.RN=1
                        ORDER BY 1;
                          """


df_cheapest_per_day = pd.read_sql_query(sqlite_cheapest_per_day,db)


