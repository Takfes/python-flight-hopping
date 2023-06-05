
# !pip install neo4j
# !pip install py2neo
import itertools
import datetime as dt
import unirest
import pandas as pd
from pandas import DataFrame
import time
import matplotlib.pyplot as plt
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

mylogfile = './logs/log_'+str(dt.datetime.now().now())[:19].replace(":","").replace(" ","h").replace("-","")+'.txt'

handler = logging.FileHandler(mylogfile)
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)

# -------------------------------------------------------------------
# -------------------------------------------------------------------

def create_session(dep_date,origin,destination,only_direct = False):
  """
  :param dep_date = "2019-06-28"
  :param origin = "ATH-sky"
  :param destination = "LCA-sky"
  :return:
  """

  url = "https://skyscanner-skyscanner-flight-search-v1.p.rapidapi.com/apiservices/pricing/v1.0"
  # url = "http://localhost:1990/"
  # url = "https://6baabfbf.ngrok.io/"

  try:
    response = unirest.post(url,
                headers={
                  "X-RapidAPI-Host": "skyscanner-skyscanner-flight-search-v1.p.rapidapi.com",
                  "X-RapidAPI-Key": "ad73627d35msh92825aa5123502cp164fd1jsn0c15a7052db0",
                  "Content-Type": "application/x-www-form-urlencoded"
                          },
                params={
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

    if response.code not in [200, 201]:
        print "Error with response code : {}".format(response.code)
    elif response.code in [200, 201]:
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
    print e

# -------------------------------------------------------------------
# -------------------------------------------------------------------

def get_flights(url):
  try:
    response = unirest.get(url,
      headers={
        "X-RapidAPI-Host": "skyscanner-skyscanner-flight-search-v1.p.rapidapi.com",
        "X-RapidAPI-Key": "ad73627d35msh92825aa5123502cp164fd1jsn0c15a7052db0",

                }
              )
    if response:
      return response.body
  except Exception as e:
    print e

# -------------------------------------------------------------------
# -------------------------------------------------------------------

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

  for itenerary in input_dict['Itineraries']:
    for y in itenerary['PricingOptions']:
      new_dict['OutboundLegId'].append(itenerary['OutboundLegId'])
      new_dict['Agents'].append(y['Agents'][0])
      new_dict['QuoteAgeInMinutes'].append(y['QuoteAgeInMinutes'])
      new_dict['Price'].append(y['Price'])
      new_dict['DeeplinkUrl'].append(y['DeeplinkUrl'])
  return DataFrame(new_dict)

# -------------------------------------------------------------------
# -------------------------------------------------------------------

def parse_data(results):

  # TODO : rewrite this
  # TODO : add error handling
  # /////////////////////////////////////////////////
  # /////////////////////////////////////////////////
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

# -------------------------------------------------------------------
# -------------------------------------------------------------------


def add_feadures(df):
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


# -------------------------------------------------------------------
# -------------------------------------------------------------------

def safely_first(date,origin,destination):
    i=0
    success = False
    while i<6 and success == False:
        print(20 * "#")
        print('SAFELY_FIRST: attempt no {}/{}'.format(i+1,6))
        start = time.time()
        next_url = create_session(date, origin, destination, only_direct=True)
        if next_url:
            success = True
            return next_url
        if i == 5:
            logger.warning('Did not manage to get data for : FROM : {} TO : {} ON : {}'.format(origin, destination, date))
            # with open(mylogfile, "a") as myfile:
            #     myfile.write('did not manage to get data for : FROM : {} TO : {} ON : {}'.format(origin, destination, date))
        i+=1

# -------------------------------------------------------------------
# -------------------------------------------------------------------

def safely_second(next_url,origin,destination,date):
    i = 0
    success = False
    while i<6 and success == False:
        print(20*"*")
        print('SAFELY_SECOND: attempt no {}/{}'.format(i+1,6))
        start = time.time()
        results = get_flights(next_url)
        if results:
            try:
                df1 = parse_data(results)
                df2 = add_feadures(df1)
                if df2.shape[0]!=0:
                    filename = './csv/{}_{}_{}_{}.csv'.format(origin, destination, date, dt.datetime.now().date()).replace("-", "")
                    success = True
                    df2.to_csv(filename)
                    print("-->Success in {} secs from start!<--".format(round(time.time() - start, 0)))
                else:
                    if i == 5:
                        logger.warning('EMPTY DATA : did not manage to get data for : FROM : {} TO : {} ON : {}'.format(origin, destination, date))
            except Exception as e:
                logger.warning(e)
        else:
            print('Error during {} attempt, {} from start... {} attempts to go... preparing for attempt no {}'.format(i,round(time.time() - start,0),5 - i,i + 1))
        if i == 5:
            # print('did not manage to get data for : FROM : {} TO : {} ON : {}'.format(origin, destination, date))
            # with open(mylogfile, "a") as myfile:
            #     myfile.write('did not manage to get data for : FROM : {} TO : {} ON : {}'.format(origin, destination, date))
            logger.warning('Did not manage to get data for : FROM : {} TO : {} ON : {}'.format(origin, destination, date))
        i+=1

# -------------------------------------------------------------------
# -------------------------------------------------------------------

def make_grid(start_date, end_date, airports):
    mylist = []
    date_range = pd.date_range(start_date, end_date)
    for r in itertools.product(date_range, airports, airports):
        if r[1] != r[2]:
            mylist.append((r[0].date(),r[1],r[2]))
    return mylist
