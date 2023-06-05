import os
os.chdir(r'C:\Users\Takis\Google Drive\_projects_\flight-hopping')

import datetime as dt
import pandas as pd
from pandas import DataFrame
import itertools
import functools
import time
import requests
import json

def timer(func):
    """
    Print the runtime of the decorated function
    """
    @functools.wraps(func)
    def wrapper_timer(*args,**kwargs):
        start_time = time.perf_counter()
        value = func(*args,**kwargs)
        end_time = time.perf_counter()
        run_time = end_time - start_time
        print(f"Finished {func.__name__!r} in {run_time:.4f} secs")
        return value
    return wrapper_timer


def create_session(dep_date,origin,destination,only_direct = True):
  """
  :param dep_date = "2019-10-28"
  :param origin = "ATH-sky"
  :param destination = "LCA-sky"
  :return:
  """

  url = "https://skyscanner-skyscanner-flight-search-v1.p.rapidapi.com/apiservices/pricing/v1.0"
  # url = "http://localhost:1990/"
  # url = "https://6baabfbf.ngrok.io/"

  try:
    response = requests.post(url,
                headers={
                  "X-RapidAPI-Host": "skyscanner-skyscanner-flight-search-v1.p.rapidapi.com",
                  "X-RapidAPI-Key": "ad73627d35msh92825aa5123502cp164fd1jsn0c15a7052db0",
                  "Content-Type": "application/x-www-form-urlencoded"
                          },
                data={
                  # "inboundDate": "2019-07-10",
                  "cabinClass": "economy",
                  "children": 0,
                  "infants": 0,
                  "country": "GR",
                  "currency": "EUR",
                  "locale": "en-US",
                  "originPlace": origin,
                  "destinationPlace": destination,
                  "outboundDate": dep_date,
                  "adults": 1
                          }
                        )

    if response.status_code not in [200, 201]:
        print("Error with response status_code : {}".format(response.status_code))
    elif response.status_code in [200, 201]:
        # get part of the url to form next url
        full_location = response.headers.get('location')
        location_part = full_location.split("/")[-1]

        # prepare url for next search
        new_url_prep = "/".join(url.split("/")[:-1]) + "/uk2/" + url.split("/")[-1] + "/"
        new_url = new_url_prep + location_part

        # for direct flights only
        new_direct_url = new_url + "?stops=0"

        if only_direct == True:
            return new_direct_url
        else:
            return new_url

  except Exception as e:
    print(e)


def make_itineraries_df_from_results(input_dict):
    """
    :param input_dict: the dictionary returned from the get flights
    :return:
    """
    # new_dict = dict.fromkeys(['OutboundLegId','Agents','QuoteAgeInMinutes','Price','DeeplinkUrl'], [])

    new_dict = {}
    new_dict['OutboundLegId']=[]
    new_dict['Agents']=[]
    new_dict['QuoteAgeInMinutes']=[]
    new_dict['Price']=[]
    new_dict['DeeplinkUrl']=[]

    try :
        for itenerary in input_dict['Itineraries']:
            for y in itenerary['PricingOptions']:
                new_dict['OutboundLegId'].append(itenerary['OutboundLegId'])
                new_dict['Agents'].append(y['Agents'][0])
                new_dict['QuoteAgeInMinutes'].append(y['QuoteAgeInMinutes'])
                new_dict['Price'].append(y['Price'])
                new_dict['DeeplinkUrl'].append(y['DeeplinkUrl'])
    except:
        pass
    finally:
        return DataFrame(new_dict)


def get_flights(url):
  try:
    response = requests.get(url,
      headers={
        "X-RapidAPI-Host": "skyscanner-skyscanner-flight-search-v1.p.rapidapi.com",
        "X-RapidAPI-Key": "ad73627d35msh92825aa5123502cp164fd1jsn0c15a7052db0",
                }
              )
    if response:
      json_acceptable_string = response.text.replace("'Hare"," Hare").replace("'", "\"")
      output_dict = json.loads(json_acceptable_string)
      return output_dict
  except Exception as e:
    print(e)


@timer
def safely_first(date,origin,destination,tries=6):
    i=0
    success = False
    while i < tries and success == False:
        # print(20 * "#")
        # print(date,origin,destination)
        # print('SAFELY_FIRST: attempt no {}/{}'.format(i+1,tries))
        # start = time.time()
        next_url = create_session(date, origin, destination, only_direct=True)
        if next_url:
            success = True
            return next_url
        if i == tries-1:
            pass
            # logger.warning('Did not manage to get data for : FROM : {} TO : {} ON : {}'.format(origin, destination, date))
            # print('Did not manage to get data for : FROM : {} TO : {} ON : {}'.format(origin, destination, date))
            # with open(mylogfile, "a") as myfile:
            #     myfile.write('did not manage to get data for : FROM : {} TO : {} ON : {}'.format(origin, destination, date))
        i += 1
    return next_url


@timer
def safely_second(next_url, tries=1):
    i = 0
    success = False
    while i < tries and success == False:
        # print(20 * "*")
        # print(f'next url is : {next_url}')
        # print('SAFELY_SECOND: attempt no {}/{}'.format(i + 1, 6))
        try:
            results = get_flights(next_url)
            i += 1
            if results:
                return results
        except Exception as e:
            pass
            # print(e)


@timer
def one_two(date,origin,destination):
    A = safely_first(date,origin,destination)
    B = safely_second(A, tries=1)
    return B


def parse_data(results):

    try:
        df_itin = make_itineraries_df_from_results(results)
        df_legs = DataFrame(results['Legs'])
        df_legs['Direct_flight_flag'] = df_legs['Stops'].apply(lambda x: True if len(x) == 0 else False)
        df_segs = DataFrame(results['Segments'])
        df_places = DataFrame(results['Places'])
        df_carr = DataFrame(results['Carriers'])
        df_agnt = DataFrame(results['Agents'])

        # /////////////////////////////////////////////////
        # /////////////////////////////////////////////////
        df_segments = df_segs. \
        merge(df_carr[['Id', 'ImageUrl', 'Name']], left_on='Carrier', right_on='Id'). \
        rename(columns={'Id_x': 'Id'
        , 'Name': 'Name_Carrier'
        , 'ImageUrl': 'ImageUrl_Carrier'
                        }). \
        drop(['Id_y'], axis=1). \
        merge(df_carr[['Id', 'ImageUrl', 'Name']], left_on='OperatingCarrier', right_on='Id'). \
        rename(columns={'Id_x': 'Id'
        , 'Name': 'Name_OperatingCarrier'
        , 'ImageUrl': 'ImageUrl_OperatingCarrier'
                        }). \
        drop(['Id_y'], axis=1). \
        merge(df_places[['Code', 'Name', 'Id']], left_on='DestinationStation', right_on='Id'). \
        rename(columns={'Id_x': 'Id'
        , 'Name': 'Destination_Name'
        , 'Code': 'Destination_Code'
                        }). \
        drop(['Id_y'], axis=1). \
        merge(df_places[['Code', 'Name', 'Id']], left_on='OriginStation', right_on='Id'). \
        rename(columns={'Id_x': 'Id'
        , 'Name': 'Origin_Name'
        , 'Code': 'Origin_Code'
                        }). \
        drop(['Id_y'], axis=1)

        # /////////////////////////////////////////////////
        #/////////////////////////////////////////////////
        df_itineraries_full = df_itin. \
        merge(df_agnt[['Id', 'ImageUrl']], left_on='Agents', right_on='Id'). \
        drop('Id', axis=1). \
        merge(df_legs, left_on='OutboundLegId', right_on='Id')

        df_itineraries_direct = df_itineraries_full[df_itineraries_full.Direct_flight_flag == True]
        df_itineraries_direct['SegmentIds'] = df_itineraries_direct.SegmentIds.apply(lambda x: x[0])
        df_itineraries_direct = df_itineraries_direct. \
        merge(df_segments, left_on='SegmentIds', right_on='Id')

        df_final = df_itineraries_direct[["DeeplinkUrl"
                                        ,"Price"
                                        ,"QuoteAgeInMinutes"
                                        ,"Arrival"
                                        ,"Departure"
                                        ,"FlightNumber"
                                        ,"ImageUrl_Carrier"
                                        ,"Name_Carrier"
                                        ,"ImageUrl_OperatingCarrier"
                                        ,"Name_OperatingCarrier"
                                        ,"Origin_Code"
                                        ,"Origin_Name"
                                        ,"Destination_Code"
                                        ,"Destination_Name"
                                        ]]

        return df_final
    except:
        pass


@timer
def add_features(df):
    try:
        prep = df.copy()

        prep.reset_index(inplace=True)

        prep['Arrival_date'] = pd.to_datetime(prep.Arrival).dt.date
        prep['Arrival_time'] = pd.to_datetime(prep.Arrival).dt.time

        prep['Departure_date'] = pd.to_datetime(prep.Departure).dt.date
        prep['Departure_time'] = pd.to_datetime(prep.Departure).dt.time

        prep['Update_date'] = dt.datetime.now().date()
        prep['Update_full'] = dt.datetime.now()

        prep.index.name = 'id'

        return prep
    except:
        pass


def make_grid(start_date, end_date, airports):
    mylist = []
    date_range = [str(x)[:10] for x in pd.date_range(start_date, end_date)]
    for r in itertools.product(date_range, airports, airports):
        if r[1] != r[2]:
            mylist.append((r[0],r[1],r[2]))
    return mylist


def enhance_grid(start_date, end_date, airports, new_airports):
    mylist = []
    date_range = [str(x)[:10] for x in pd.date_range(start_date, end_date)]
    for r in itertools.product(date_range, airports, new_airports):
        if r[1] != r[2] and r[2] in new_airports:
                mylist.append((r[0], r[1], r[2]))
                mylist.append((r[0], r[2], r[1]))
    return mylist


def flights_to_frame(df):
    dataframe_list = []
    for x in df.dask_api_results:
        df01 = parse_data(x)
        df02 = add_features(df01)
        dataframe_list.append(df02)
    final_df = pd.concat(dataframe_list)
    return final_df


# ==============================================
# START
# ==============================================

import psycopg2 as pg
from sqlalchemy import create_engine
con = create_engine("postgresql://postgres:postgres@localhost:5432")

query = r'SELECT DISTINCT "Origin_Code" FROM public.FLIGHTS ORDER BY 1'
query_results = pd.read_sql_query(query, con)
airports = [x+"-sky" for x in query_results.Origin_Code]

# airports = ['CDG-sky','MAD-sky','CPH-sky','FCO-sky','AMS-sky','LHR-sky','ATH-sky']
# new_airports = ['BCN-sky','VIE-sky','VNO-sky','LIS-sky']
# new_airports = ['IST-sky','ARN-sky','WAW-sky','BSL-sky']

# new_airports = ['RAK-sky','DUB-sky','OSL-sky','DBV-sky','ZAG-sky','LED-sky']
new_airports = ['MXP-sky','DUB-sky','VCE-sky', 'ZAG-sky', 'KRK-sky','KTW-sky']


# INITIATE GRID
# grid = make_grid('2020-03-01','2020-04-01',airports)
# gridf = pd.DataFrame(grid,columns=['depdate','origin','destination'])
# gridf.shape

# ADD TO A GRID
grid = enhance_grid('2020-03-01','2020-04-01',airports, new_airports)
gridf = pd.DataFrame(grid,columns=['depdate','origin','destination'])
gridf.shape

gridf.to_clipboard()

# ==============================================
# make calls with PANDAS apply
# ==============================================

# start = time.time()
# gridfs['api_results'] = gridfs.apply(lambda x : one_two(x['depdate'],x['origin'],x['destination']), axis = 1)
# end = time.time()
# print(f'duration : {end-start}')
# # 461.2800524234772
#
# gridfs.shape
# gridfs.api_results.isnull().sum()
# gridfsc = gridfs.loc[gridfs.api_results.notnull(),:]

# ==============================================
# make calls with DASK apply
# ==============================================

import multiprocessing
import dask.dataframe as dd
from dask.distributed import Client
client = Client()
client

start1 = time.time()
gridf['dask_api_results'] = dd.from_pandas(gridf, npartitions=5*multiprocessing.cpu_count()).\
    map_partitions(lambda df : df.apply((lambda row : one_two(row['depdate'],row['origin'],row['destination'])),axis=1)).\
    compute(scheduler='processes')
end1 = time.time()
print(f'Process took {end1-start1}')
# 89.40432238578796

gridf.shape
gridf['dask_api_results'].isnull().sum()
gridf['dask_api_results'].notnull().sum()
gridfc = gridf.loc[gridf['dask_api_results'].notnull()]
gridfc.shape

# ==============================================
# DATAFRAME TO STANDARD FORMAT
# ==============================================

start2 = time.time()
uu = flights_to_frame(gridfc)
end2 = time.time()
print(f'Process took {end2-start2}') # 6.791619539260864

uu.shape
uu.to_clipboard()
uu.to_csv("./DockerShare/flights_17122019_EDIOPOLISKEFBIOAGP.csv",index =False)

