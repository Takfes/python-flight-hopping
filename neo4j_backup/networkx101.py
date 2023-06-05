
# ------------------------------------------------------------------------------------------------------
# ------------------------------ NETWORKX BASICS -------------------------------------------------------
# ------------------------------------------------------------------------------------------------------

import networkx as nx
import matplotlib.pyplot as plt
import pandas as pd
import datetime as dt

#g = nx.Graph() # nx.DiGraph() # MultiDiGraph
#
#g.add_node(1, airport = 'MAD')
#g.add_node(2, airport = 'ATH')
#g.add_node(3)
#
#g.nodes()
#g.nodes().data()
#g.nodes[4]['airport'] = "LHR"
#g.nodes[2]['day'] = '2019-02-09'
#
#
#for x in g.nodes(data=True): #g.nodes().data() or g.nodes(data=True)
#    print(x)
#
#
#g.add_node('abc', day=1185, airport='usa', dayob='monday')


#nx.set_node_attributes(g , node_attrs) this affect the entire graph

#lel = datetime.datetime(2019,9,1)
#node_attrs = {'airport' : 'ATH', 'date': lel }
#node_attrs = {'airport' : 'ATH', 'date': datetime.datetime(2019,9,1) }
#g.add_node('cde', {'dob': 1185, 'pob': 'usa', 'dayob': 'monday'}) this does not work


#g.add_edge(2,3, info = 'stays')
#g.add_edge(4,5, info = 'stays', more = '20190811', cost = '123')
#g.add_edges_from([(1,4),(3,4)])
#
#
#
#for x in g.edges(data=True): #g.nodes().data() or g.nodes(data=True)
#    print(x)
#
#
#nx.info(g)
#
#nx.number_of_nodes(g)
#nx.number_of_edges(g)
#
#nx.draw(g)
#
#nx.is_directed(g)


# ------------------------------------------------------------------------------------------------------
# ------------------------------ GET RAW FLIGHTS DATA --------------------------------------------------
# ------------------------------------------------------------------------------------------------------


#from py2neo import Graph, Database, Node, Relationship
import os, time, sqlite3
import pandas as pd
import numpy as np

os.chdir("C:\\Users\\takis\\Desktop\\Dockerneo4j")
os.getcwd()


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

# FETCH DATA
df = pd.read_sql_query(user_query,db)
df.shape
df.columns

fetched_data = df.copy()
df = fetched_data.copy()

df['AirportDay_start'] = (df['Origin_Code']      + '_' + df['Departure_date']).str.replace("-","")
df['AirportDay_end']   = (df['Destination_Code'] + '_' + df['Arrival_date']).str.replace("-","")


start_date = pd.to_datetime(df['Departure_date']).min()
end_date   = pd.to_datetime(df['Departure_date']).max()
date_range = pd.date_range(start_date, end_date)



gg = nx.MultiDiGraph()

for index, row in df.iterrows():
    Departure=row['Departure']
    Arrival=row['Arrival']
    Origin_Code=row['Origin_Code']
    Origin_Name=row['Origin_Name']
    Destination_Code=row['Destination_Code']
    Destination_Name=row['Destination_Name']
    Name_Carrier=row['Name_Carrier']
    Name_OperatingCarrier=row['Name_OperatingCarrier']
    FlightNumber=row['FlightNumber']
    Departure_date=row['Departure_date']
    Departure_time=row['Departure_time']
    Arrival_date=row['Arrival_date']
    Arrival_time=row['Arrival_time']
    DeeplinkUrl=row['DeeplinkUrl']
    Price=row['Price']
    CNT=row['CNT']
    QuoteAgeInMinutes=row['QuoteAgeInMinutes']
    AirportDay_start=row['AirportDay_start']
    AirportDay_end=row['AirportDay_end']
   
    gg.add_node(AirportDay_start, origin_code = Origin_Code, date = Departure_date)
    
    gg.add_edge(AirportDay_start,AirportDay_end, info = 'flight', weight = Price, 
                Departure=Departure,
                Arrival=Arrival,
                Origin_Code=Origin_Code,
                Origin_Name=Origin_Name,
                Destination_Code=Destination_Code,
                Destination_Name=Destination_Name,
                Name_Carrier=Name_Carrier,
                Name_OperatingCarrier=Name_OperatingCarrier,
                FlightNumber=FlightNumber,
                Departure_date=Departure_date,
                Departure_time=Departure_time,
                Arrival_date=Arrival_date,
                Arrival_time=Arrival_time,
                DeeplinkUrl=DeeplinkUrl,
                Price=Price,
                CNT=CNT,
                QuoteAgeInMinutes=QuoteAgeInMinutes,
                AirportDay_start=AirportDay_start,
                AirportDay_end=AirportDay_end
               )
#    # Add edges w/o iteration, not sure how to add attributes at the same time
#    # TODO : add attributes, info = 'stay' , cost = 0
#    stays = [Origin_Code+"_"+str(x)[:10].replace("-","") for x in date_range[date_range>=Departure_date]]
#    check = [(AirportDay_start,x) for x in stays]
#    gg.add_edges_from(check)

#nx.number_of_nodes(g)
#nx.number_of_edges(g)
#nx.is_directed(g)
nx.info(gg)
nx.draw(gg)
gg.edges(data=True)
gg.node['MAD_20190930']

for i,x in enumerate(gg.edges(data=True)):
    print(i)
    print(x)
    if 1 >10:
        exit

nx.shortest_path(gg, source='ATH_20190910', target='ATH_20190920', weight=Price)

p = nx.shortest_path(gg,weight=Price)
p





