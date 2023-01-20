# BE_Index

import requests
import pandas as pd
from BE_utils import *
from BE_Condition import BE_Filter
from BE_Url_Check import BE_Check_url
from datetime import datetime

def BE_Store_data(df, data, category):
  # process data and update output save it in "out"
  response, check = BE_Check_url(data['v360_link'])
  # if(check == 0):
  #   output = process(data['v360_link'])
  output = 0
  if(check == 1):
    output = "Link is not Proper"
  # listing for dataframe
  pd.concat([df, pd.Series([
      data['certificate_number'],
      data['v360_link'],
      data['create_date'],
      data['download_status'],
      data['active_status'],
      data['colored_diamonds'],
      data['resync_need'],
      category,
      output
      ])], ignore_index = True )
  
  return df

def BE_Fetch_page(page, df):
  update = 0
  for index, data in enumerate(page):
    category = BE_Filter(data)
    if(category != CATEGORIES[0]): 
      df = BE_Store_data(df, data, category)
      update += 1
  
  return update, df

def BE_Fetch_data():
  df = pd.DataFrame(columns=[
      'certificate_number',
      'v360_link',
      'create_date',
      'download_status',
      'active_status',
      'colored_diamonds',
      'resync_need',
      'category',
      'output'
  ])
  
  total_overall = 0 
  update_overall = 0

  for page in range(1,  total_pages+1):
    URL = f"https://www.brilliantearth.com/v360-api/get-v360-link/?page={page}&per_page={NUM_PER_PAGE}"
    res = requests.get(URL, json=data, headers=HEADERS)
    update, df = BE_Fetch_page(res.json()['data']['v360_links'], df)
    total = len(res.json()["data"]["v360_links"])
    total_overall += total
    update_overall += update  
    print('Page: ', res.json()["data"]["curr_page_num"], 'Total: ', total, 'Update: ' , update)

  df.to_csv(f'Logs/Log_{datetime.now().strftime("%b-%d-%Y_%H:%M:%S")}.csv')
#   print("...Filtering done")
  return update_overall, total_overall

def BE_Index(request):
    global total_pages, total_responses, data
    total_pages, total_responses, data = initialise()
    update_overall, total_overall = BE_Fetch_data()
    return "Total Response: " + total_overall + "Updated Response: " + update_overall
