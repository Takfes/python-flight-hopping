import datetime as dt
import unirest
import pandas as pd
from pandas import DataFrame
import time
import matplotlib.pyplot as plt
from api_handler import *
import itertools



# date_range = pd.date_range('2019-09-04','2019-09-06')
# airports = ['CPH-sky','ATH-sky']
airports = ['CDG-sky','MAD-sky','CPH-sky','FCO-sky','AMS-sky','LHR-sky','ATH-sky']

grid = make_grid('2019-09-01','2019-09-03',airports)
grid = make_grid('2019-09-04','2019-09-06',airports)
grid = make_grid('2019-09-07','2019-09-09',airports)
grid = make_grid('2019-09-10','2019-09-12',airports)
grid = make_grid('2019-09-13','2019-09-15',airports)
grid = make_grid('2019-09-16','2019-09-20',airports)
grid = make_grid('2019-09-21','2019-09-30',airports)


# Flight 353 out of 420
# DATE : 2019-09-29 // ORIGIN : CPH-sky // DESTINATION : LHR-sky

small_grid = grid[353:]
small_grid[0]

# len(grid)-len(small_grid)

def do_all(grid):
    total_start = time.time()
    for k,r in enumerate(grid,start=1):
        print(" ")
        print(50 * "/")
        print(" ")
        print('Flight {} out of {}'.format(k,len(grid)))
        print('DATE : {} // ORIGIN : {} // DESTINATION : {}'.format(r[0], r[1], r[2]))
        begin = time.time()
        next_url = safely_first(r[0], r[1], r[2])
        if next_url:
            safely_second(next_url, r[0], r[1], r[2])
        finish = time.time()
        print('Flight {} out of {} completed in {} secs'.format(k,len(grid),round(finish-begin,0)))
        print('Running time {} secs'.format(round(finish-total_start, 0)))
    print(" ")
    print(50 * "!")
    total_finish = time.time()
    print('Operation completed in {} mins'.format(round((total_finish-total_start)/60,1)))
    print(50 * "!")

do_all(small_grid)





from concurrent.futures import ThreadPoolExecutor, wait, as_completed

pool = ThreadPoolExecutor(len(urls))  # for many urls, this should probably be capped at some value.

futures = [pool.submit(requests.get, url) for url in urls]
results = [r.result() for r in as_completed(futures)]
print "Threadpool done in %s" % (time() - then)








