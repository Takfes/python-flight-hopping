
import os, time, sqlite3
import pandas as pd

# TODO : Make a function to parse the unparsed csvs

os.getcwd()
os.chdir("C:\\Users\\takis\\Desktop\\Dockerneo4j\\csv")
files = os.listdir()
len(files)

DB_TABLE_TO_STORE_RAW_FLIGHTS = "raw_flights"
DB_CONNECTION_STRING = '../airflights.db'
ACTION = 'append' # append, replace, fail

def parse_csv_in_db(DB_TABLE_TO_STORE_RAW_FLIGHTS,DB_CONNECTION_STRING,ACTION,files):
    failed = 0
    rows = 0
    start = time.time()
    db = sqlite3.connect(DB_CONNECTION_STRING)
    
    for i,file in enumerate(files,start=1):
        print(50*"-")
        print(i)
        print(f'Running time {round(time.time()-start,0)} secs')
        print(file)
        temp_df = pd.read_csv(file)
        if temp_df.shape[0] ==0:
            print('--->>> Empty file ! <<<---')
            failed+=1
        else:
            print(f'File not empty ! {temp_df.shape[0]} rows will be inserted in the db')
            temp_df.to_sql(DB_TABLE_TO_STORE_RAW_FLIGHTS,db,if_exists=ACTION,index=False)
            rows += temp_df.shape[0]
        print()
        print(50*"-")
    end = time.time()
    print(f'It took {round(end-start,0)} secs to store {rows} rows out of {len(files)-failed} non-empty files and {failed} number of empty itineraries ')

parse_csv_in_db(DB_TABLE_TO_STORE_RAW_FLIGHTS,DB_CONNECTION_STRING,ACTION,files)

temp_df.columns
#['id', 'index', 'DeeplinkUrl', 'Price', 'QuoteAgeInMinutes', 'Arrival',
#       'Departure', 'FlightNumber', 'ImageUrl_Carrier', 'Name_Carrier',
#       'ImageUrl_OperatingCarrier', 'Name_OperatingCarrier', 'Origin_Code',
#       'Origin_Name', 'Destination_Code', 'Destination_Name', 'Arrival_date',
#       'Arrival_time', 'Departure_date', 'Departure_time', 'Update_date',
#       'Update_full'],
#      dtype='object')

db.close()

#import os, time, sqlite3
#import pandas as pd
#
#db = sqlite3.connect('../airflights.db')
#USER_ORIGIN = "'ATH'"
#USER_DESTINATIONS = ('CDG','MAD','FCO','AMS', USER_ORIGIN)
#DEPARTURE_TIME_FROM = '"10:00:00"'
#DEPARTURE_TIME_TO = '"18:00:00"'
#
#user_query = """SELECT
#                Departure_date
#                 ,Origin_Code
#                 ,Destination_Code
#                 ,Name_Carrier
#                 ,Name_OperatingCarrier
#                 ,CAST(FlightNumber as VARCHAR) AS FlightNumber
#                 ,Departure_date
#                 ,Departure_time
#                 ,Arrival_date
#                 ,Arrival_time
#                 ,DeeplinkUrl
#                 ,MIN(Price) as Min_Price
#                 --,STDEV(Price)
#                 ,count(*) as CNT
#                 FROM flights
#                 WHERE 1=1
#                 AND Origin_Code = {}
#                 AND Destination_Code IN {}
#                 AND Departure_time BETWEEN {} AND {}
#                 GROUP BY Departure_date, Origin_Code, Destination_Code
#                 ORDER BY Departure_date, Origin_Code, Destination_Code, Departure_time
#                 LIMIT 100;
#                """.format(USER_ORIGIN,USER_DESTINATIONS,DEPARTURE_TIME_FROM,DEPARTURE_TIME_TO)
#
#import pprint
#pp = pprint.PrettyPrinter(indent=4)
#pp.pprint(user_query)
#
#count_query = 'SELECT COUNT(*) FROM flights'
#
#df = pd.read_sql_query(user_query,db)
#df = pd.read_sql_query(count_query,db)
##df = pd.read_sql(query , db)
#
#df.to_clipboard()
#
#db.close()

