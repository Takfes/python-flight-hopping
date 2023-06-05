
import unirest
import pandas as pd
from pandas import DataFrame

# There are two calls before getting any data;
# 1 first api call is where you specify the query parameters
# 2 as a response you get the location parameter in the query header
# 3 using this you hit a different endpoint to requests for the results of your query
# 4 as a response you get an object with all the information

# 1st api-endpoint to get the location parameter
URL_1 = "https://skyscanner-skyscanner-flight-search-v1.p.rapidapi.com/apiservices/pricing/v1.0"

response = unirest.post(URL_1,
  headers={
    "X-RapidAPI-Host": "skyscanner-skyscanner-flight-search-v1.p.rapidapi.com",
    "X-RapidAPI-Key": "ad73627d35msh92825aa5123502cp164fd1jsn0c15a7052db0",
    "Content-Type": "application/x-www-form-urlencoded"
  },
  params={
    "inboundDate": "2019-07-10",
    "cabinClass": "economy",
    "children": 0,
    "infants": 0,
    "country": "GR",
    "currency": "EUR",
    "locale": "en-US",
    "originPlace": "ATH-sky",
    "destinationPlace": "LCA-sky",
    "outboundDate": "2019-06-28",
    "adults": 1
  }
)

response.code
full_location = response.headers.get('location')
location_part = full_location.split("/")[-1]

# prepare second URL
url2_prep = "/".join(URL_1.split("/")[:-1])+"/uk2/"+URL_1.split("/")[-1]+"/"
URL_2 = url2_prep + location_part

# make a request to the 2nd api-endpoint
response_2 = unirest.get(URL_2,
  headers={
    "X-RapidAPI-Host": "skyscanner-skyscanner-flight-search-v1.p.rapidapi.com",
    "X-RapidAPI-Key": "ad73627d35msh92825aa5123502cp164fd1jsn0c15a7052db0"
  }
)

# response_2.code
# response_2.body

resp_dict = response_2.body
resp_dict.keys()

import pprint
pp = pprint.PrettyPrinter(indent=4)
pp.pprint(resp_dict['Legs'][0])

# from 'Legs' extract only direct flights
direct_leg_ids = [resp_dict['Legs'][x]['Id'] for x in range(len(resp_dict['Legs'])) if len(resp_dict['Legs'][0]['SegmentIds'])==1]

len(set(direct_leg_ids))
len(direct_leg_ids)

resp_dict['Itineraries']


# Legs
df_legs = DataFrame(resp_dict['Legs'])
df_legs['Direct_flight_flag'] = df_legs['Stops'].apply(lambda x: True if len(x) == 0 else False)
df_legs.head()

# Segments
df_segs = DataFrame(resp_dict['Segments'])
df_segs.head()

# Places
df_plac = DataFrame(resp_dict['Places'])
# df_plac.ParentId.fillna(0).apply(str)
df_plac.head()

# Carriers
df_carr = DataFrame(resp_dict['Carriers'])
df_carr.head()

# Agents
df_agnt = DataFrame(resp_dict['Agents'])
df_agnt.head()

# Itineraries
# TODO : parse Itineraries object
resp_dict['Itineraries']
pricing_options_list = [resp_dict['Itineraries'][x]['PricingOptions'] for x in range(len(resp_dict['Itineraries']))]
xx = DataFrame(pricing_options_list)
xx.head()
xx.to_clipboard()

# TODO : check mongodb for storing
# TODO : check noe4j for storing
