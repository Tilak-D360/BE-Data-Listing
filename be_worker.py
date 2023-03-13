# multithreading over page and data both
# main.py

import requests
from threading import Thread
import pandas as pd
from datetime import datetime
from pytz import timezone 
import traceback

from be_utils import NUM_PER_PAGE, URL, METADATA, HEADERS, CATEGORIES
from be_utils import CERTI_SET, BUCKET_NAME, LOGS_FILE, PATH_TO_CERTI_DATA
from be_utils import BE_initialise_data, BE_save_data, BE_pre_check

from be_condition import BE_filter
from be_url_checker import BE_url_res_checker

COLUMNS = [
    'certificate_number', 
    'vendor_name',
    'v360_link', 
    'create_date', 
    'download_status', 
    'active_status', 
    'colored_diamonds', 
    'resync_need'
    ]

BAD_DOMAIN = set()
SUFFIX = set()
ROWS = []    # to store all ROWS parellely
PAGE_RESULTS = []
UPDATE_IDS = {}
RESYNCED_ROWS = []
RESYNCED_UPDATE_IDS = {}
SUB_CORES = 100      # 100 cores will not allowable over cloud function
CORES = 5

# function to initialze vars and dictionaries
def BE_initialise_vars(DATA):
    if 'suffix' in DATA:
        for i in DATA['suffix']:
            UPDATE_IDS[f'{i}'] = []
            RESYNCED_UPDATE_IDS[f'{i}'] = []
            SUFFIX.add(str(i))

# reseting dataframes which will be saved over bucket
def BE_reset_dfs(logs_df, ids_df):
    logs_df = pd.DataFrame(ROWS)
    logs_df.append(RESYNCED_ROWS)
    for suffix in SUFFIX: 
        ids_df[suffix] = pd.DataFrame(UPDATE_IDS[suffix])
        if suffix in RESYNCED_UPDATE_IDS:
            ids_df[suffix].append(RESYNCED_UPDATE_IDS[suffix]) 
    return logs_df, ids_df

# preparing dataframes which will be saved as csv over bucket
def BE_update_dfs(logs_df, ids_df, data_df, is_reset = False):
    data_df = pd.DataFrame(SUFFIX, columns=['suffix'])
    if(is_reset == True):
        logs_df, ids_df = BE_reset_dfs(logs_df, ids_df)
        return logs_df, ids_df, data_df

    logs_df = logs_df.append(ROWS)
    ALREADY_DONE = set(logs_df['certificate_number'])
    for row in RESYNCED_ROWS:
        certi = row['certificate_number']
        if(certi in ALREADY_DONE):
            logs_df.loc[
                logs_df['certificate_number'] == certi, 
                ['category', 'status', 'last_check_datetime', 'last_check']
                ] = row['category'], row['status'], row['last_check_datetime'], row['last_check']
        else:
            logs_df = logs_df.append(row, ignore_index = True)

    for suffix, df in ids_df.items():
        suffix = str(suffix)
        if(suffix not in ids_df):
            ids_df[suffix] = pd.DataFrame()
        ids_df[suffix] = ids_df[suffix].append(UPDATE_IDS[suffix], ignore_index = True)
        ALREADY_DONE = set(ids_df[suffix]['certificate_number'])
        for row in RESYNCED_UPDATE_IDS[suffix]:
            certi = row['certificate_number']
            if(certi in ALREADY_DONE):
                ids_df[suffix].loc[ 
                    ids_df[suffix]['certificate_number'] == certi, 
                    'Last Check date_time(IST)'
                    ] = row['Last Check date_time(IST)']
            else:
                ids_df[suffix] = ids_df[suffix].append(row, ignore_index = True)
    print('-'*6 , ' dataframes updated successfully ', '-'*6 )
    return logs_df, ids_df, data_df

# function to update the details like adding their certificate number to set CERTI_SET
# and adding certificate number and datetime into data which will be saved in files
def BE_update_data(data, category, suffix):
  
  # adding certificate number into set
  if(suffix in CERTI_SET):    
    CERTI_SET[suffix].add(data['certificate_number'])
  else:
    CERTI_SET[suffix] = set(data['certificate_number'])  
    SUFFIX.add(suffix)

  # updating data required to enter into file of certificate numbers
  id = {
    'certificate_number' : data['certificate_number'],
    'Last Check date_time(IST)' : datetime.now(timezone("Asia/Kolkata")).strftime("%d-%b-%Y %H:%M:%S")
    }
  
  if(category == CATEGORIES[1]):
    if(suffix not in RESYNCED_UPDATE_IDS): 
        RESYNCED_UPDATE_IDS[suffix] = []
    RESYNCED_UPDATE_IDS[suffix].append(id)
    return
  
  if(suffix not in UPDATE_IDS): 
    UPDATE_IDS[suffix] = []
  UPDATE_IDS[suffix].append(id)
  return
  

# function to process rows and update datetime for that updation
# and prepare data which are needed to be saved into files accordingly
def BE_process_row(data, category):
  row = dict()
  for col in COLUMNS:
    row[col] = data[col]
  row['category'] = category
  link = data['v360_link']
  row['status'] = BE_url_res_checker(link)
  row['last_check_datetime'] = "IST: " + datetime.now(timezone("Asia/Kolkata")).strftime("%b-%d-%Y_%H:%M:%S")
  row['last_check'] = 'Y' 
  suffix = str(data['certificate_number'][-1])
  if(category == CATEGORIES[1]):
    RESYNCED_ROWS.append(row)
  else:
    ROWS.append(row)

  BE_update_data(data, category, suffix)
  
  return
  
# function which will multithread over data of a page 
def BE_Fetch_page(page_num, page_data, index, is_reset = False):
  THREADS = []  # page details for multiprocessing
  update = 0
  try:
    for data in page_data:
      suffix = str(data['certificate_number'][-1])
      category = BE_filter(data)
      if((not is_reset) and ((suffix in SUFFIX) and (data['certificate_number'] in CERTI_SET[suffix])) and (category != CATEGORIES[1])):
        continue
      
      if(category != CATEGORIES[0]): 
        t = Thread(target = BE_process_row, args = (data, category))
        t.start()
        THREADS.append(t)
        update += 1
        if ( len(THREADS) != 0 and len(THREADS) % SUB_CORES == 0):
          for t in THREADS:
            t.join()
          THREADS = []
    
    if (len(THREADS) != 0):
      for t in THREADS:
        t.join()
      THREADS = []
      
    PAGE_RESULTS[index]['update'] = update

  except Exception as e:
    print(f'error {e} in fetch page{page_num}', traceback.format_exc())
    PAGE_RESULTS[index]['update'] = update

# function to manage page thread complete action and printing details 
def BE_wait_for_page_threads(THREADS, total, update):
    for t in THREADS:
        t.join()
    for res in PAGE_RESULTS[ -len(THREADS): ]:
        print('Page: ', res['page'], 'Total: ', res['total'], 'Update: ' , res['update'])
        total += res['total']
        update += res['update']
    return total, update

# function to multithread over pages
def BE_Fetch_data(total_pages, is_reset = False, start = 1):
    THREADS = []
    total_overall = 0 
    update_overall = 0

    try:
        for page in range(start,  total_pages + 1):
            try:
                URL = f"https://www.brilliantearth.com/v360-api/get-v360-link/?page={page}&per_page={NUM_PER_PAGE}"
                res = requests.get(URL, json=METADATA, headers=HEADERS)
                data = res.json()['data']['v360_links']
            except:  # error handeling for pagination fault
                print(f"!!! Pagination Fault for {page} !!!")
                if(len(THREADS) != 0):
                    print('Waiting for last of page threads')
                    total_overall, update_overall = BE_wait_for_page_threads(THREADS, total_overall, update_overall)
                    THREADS = []
                return total_overall, update_overall
                
            else:
                sample = {'page' : page, 'total' : len(data), 'update' : 0}
                t = Thread(target = BE_Fetch_page, args = (page, data, page - start))
                PAGE_RESULTS.append(sample)
                t.start()
                THREADS.append(t)
            finally:
                if(len(THREADS) != 0 and len(THREADS) % CORES == 0):
                    print('Waiting for page threads')
                    total_overall, update_overall = BE_wait_for_page_threads(THREADS, total_overall, update_overall)
                    THREADS = []
                
        if(len(THREADS) != 0):
            print('Waiting for last of page threads')
            total_overall, update_overall = BE_wait_for_page_threads(THREADS, total_overall, update_overall)
            THREADS = []

    except Exception as e:
        print(f'fetch_data_error on {page}', e)
    finally:
        print('-'*6 , ' data fetching done ', '-'*6 )
        return update_overall, total_overall
