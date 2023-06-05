
# -------------------------------------------------------------------
# TIMER
# -------------------------------------------------------------------

import functools
import time

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


@timer
def do_500():
    for x in range(50):
        print(x)

do_500()


# -------------------------------------------------------------------
# RETRY
# -------------------------------------------------------------------


from retrying import retry

def retry_if_result_none(result):
    """Return True if we should retry (in this case when result is None), False otherwise"""
    return result is None

@retry(retry_on_result=retry_if_result_none, stop_max_attempt_number=7)
def might_return_none():
    for x in range(11):
        if x != 10:
            retry_if_result_none(None)
        else:
            print("ACHIEVED")


might_return_none()

# -------------------------------------------------------------------
# RETRY
# -------------------------------------------------------------------

import time
from functools import wraps


def retry(exceptions, tries=4, delay=3, backoff=2, logger=None):
    """
    Retry calling the decorated function using an exponential backoff.

    Args:
        exceptions: The exception to check. may be a tuple of
            exceptions to check.
        tries: Number of times to try (not retry) before giving up.
        delay: Initial delay between retries in seconds.
        backoff: Backoff multiplier (e.g. value of 2 will double the delay
            each retry).
        logger: Logger to use. If None, print.
    """
    def deco_retry(f):

        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except exceptions as e:
                    msg = '{}, Retrying in {} seconds...'.format(e, mdelay)
                    if logger:
                        logger.warning(msg)
                    else:
                        print(msg)
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)

        return f_retry  # true decorator

    return deco_retry

# -------------------------------------------------------------------
# -------------------------------------------------------------------

from multiprocessing.pool import ThreadPool

results = ThreadPool(8).imap_unordered(do_all, grid)
for path in results:
    print(path)
print(f"Elapsed Time: {timer() - start}")



# -------------------------------------------------------------------
# -------------------------------------------------------------------



threads = []
# In this case 'urls' is a list of urls to be crawled.
for ii in range(len(urls)):
    # We start one thread per url present.
    process = Thread(target=crawl, args=[urls[ii], result, ii])
    process.start()
    threads.append(process)
    
    
# -------------------------------------------------------------------
# -------------------------------------------------------------------

dep_date = "2019-06-28"
origin = "ATH-sky"
destination = "LCA-sky"

fetch(dep_date,origin,destination,only_direct = False)


@retry
def fetch(dep_date,origin,destination,only_direct = False):
	"""
	:param dep_date = "2019-06-28"
	:param origin = "ATH-sky"
	:param destination = "LCA-sky"
	:return:
	"""

	url = "https://skyscanner-skyscanner-flight-search-v1.p.rapidapi.com/apiservices/pricing/v1.0"
	# url = "http://localhost:1990/"
	# url = "https://6baabfbf.ngrok.io/"

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
		raise Exception("Error with response status_code : {}".format(response.status_code))
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
			print(new_direct_url)
		else:
			return new_url
			print(new_url)

# -------------------------------------------------------------------
# -------------------------------------------------------------------

def break_list(start_date, end_date, airports):
    mylist = []
    date_range = pd.date_range(start_date, end_date)
    date_range = [str(x)[:10] for x in date_range]
    for r in itertools.product(date_range, airports, airports):
        if r[1] != r[2]:
            mylist.append("&&".join([r[0],r[1],r[2]]))
    return mylist


parallel_list = break_list(start_date, end_date, airports)

# -------------------------------------------------------------------
# -------------------------------------------------------------------

def safely_first(all_in_one_string):
    i=0
    success = False
    date = all_in_one_string.split("&&")[0]
    origin = all_in_one_string.split("&&")[1]
    destination = all_in_one_string.split("&&")[2]
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

def safely_second(next_url,all_in_one_string):
    i = 0
    success = False
    date = all_in_one_string.split("&&")[0]
    origin = all_in_one_string.split("&&")[1]
    destination = all_in_one_string.split("&&")[2]
    while i<1 and success == False:
        print(20*"*")
        print('SAFELY_SECOND: attempt no {}/{}'.format(i+1,6))
        start = time.time()
        
        try:
            results = get_flights(next_url)
        except Exception as e:
            print(e)
            return
        
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
            print('Error during {} attempt, {} from start... {} attempts to go... preparing for attempt no {}'.format(i+1,round(time.time() - start,0),5 - i,i + 2))
        if i == 5:
            # print('did not manage to get data for : FROM : {} TO : {} ON : {}'.format(origin, destination, date))
            # with open(mylogfile, "a") as myfile:
            #     myfile.write('did not manage to get data for : FROM : {} TO : {} ON : {}'.format(origin, destination, date))
            logger.warning('Did not manage to get data for : FROM : {} TO : {} ON : {}'.format(origin, destination, date))
        i+=1

# -------------------------------------------------------------------
# -------------------------------------------------------------------

def do_all(grid):
    total_start = time.time()
    for k,r in enumerate(grid,start=1):
        print(" ")
        print(50 * "/")
        print(" ")
        print(r)
#        print('Flight {} out of {}'.format(k,len(grid)))
#        print('DATE : {} // ORIGIN : {} // DESTINATION : {}'.format(r[0], r[1], r[2]))
        begin = time.time()
        next_url = safely_first(r)
        if next_url:
            safely_second(r)
        finish = time.time()
#        print('Flight {} out of {} completed in {} secs'.format(k,len(grid),round(finish-begin,0)))
#        print('Running time {} secs'.format(round(finish-total_start, 0)))
    print(" ")
    print(50 * "!")
    total_finish = time.time()
    print('Operation completed in {} mins'.format(round((total_finish-total_start)/60,1)))
    print(50 * "!")
    
    
# -------------------------------------------------------------------
# -------------------------------------------------------------------

from concurrent.futures import ThreadPoolExecutor, wait, as_completed
import socket

pool = ThreadPoolExecutor(len(grid))  # for many urls, this should probably be capped at some value.

with PoolExecutor(max_workers=2) as executor:
    for _ in executor.map(do_all, parallel_list):
        pass

# -------------------------------------------------------------------
# -------------------------------------------------------------------

futures = [pool.submit(requests.get, url) for url in urls]
results = [r.result() for r in as_completed(futures)]
print "Threadpool done in %s" % (time() - then)


