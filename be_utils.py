# BE_utils
import requests
from google.cloud import storage
import pandas as pd

NUM_PER_PAGE = 5000
URL = f"https://www.brilliantearth.com/v360-api/get-v360-link/?page=1&per_page={NUM_PER_PAGE}"
METADATA = {"username": "beteam", "password": "Cacti17orange", "showPassword": "false"}
HEADERS = {'AUTH-TOKEN': 'TElWRV9WMzYwX0xJTktfSlNPTl9UT0tFTg=='}
CATEGORIES = ('no_update', 'resync_needY', 'activeY_downloadN', 'activeN_downloadN_coloredDiamondY')
BUCKET_NAME = 'be-data-listing-bucket'
LOGS_FILE = 'logs.csv'
DATA_FILE = 'suffix-data.csv'
PATH_TO_CERTI_DATA = 'certi_ids/Certi_ids:'

total_pages, total_responses = 0, 0
CERTI_SET = {}
# reading and writing over bucket

def BE_initialise_data(is_reset = False):
    try:
        if is_reset == False:
            LOGS = pd.read_csv(f'gs://{BUCKET_NAME}/{LOGS_FILE}')
            LOGS.loc[:,'last_check'] = ''
            DATA = pd.read_csv(f'gs://{BUCKET_NAME}/{DATA_FILE}')
            IDS = {}
            for suffix in DATA['suffix']:
                suffix = str(suffix)
                IDS[suffix] = pd.read_csv(f'gs://{BUCKET_NAME}/{PATH_TO_CERTI_DATA}{suffix}.csv')
                CERTI_SET[suffix] = set(IDS[suffix]['certificate_number'])
            return LOGS, IDS, DATA
        LOGS = pd.DataFrame()
        DATA = pd.DataFrame()
        IDS = {}
        return LOGS, IDS, DATA

    except Exception as e:
        print("!!! error in data fetching from bucket !!!")
        print("!!! check required dependencies !!!")
        print("Error: ", e)

def BE_save_data(logs_df, ids_df, data_df, is_flag = False):
    try:
        if(is_flag):
            logs_df.to_csv(f'gs://{BUCKET_NAME}/flag_{LOGS_FILE}', index=False)
            data_df.to_csv(f'gs://{BUCKET_NAME}/flag_{DATA_FILE}', index=False)
            for suffix, df in ids_df.items():
                df.to_csv(f'gs://{BUCKET_NAME}/flag_{PATH_TO_CERTI_DATA}{suffix}.csv', index=False)
        else:
            logs_df.to_csv(f'gs://{BUCKET_NAME}/{LOGS_FILE}', index = False)
            data_df.to_csv(f'gs://{BUCKET_NAME}/{DATA_FILE}', index = False)
            for suffix, df in ids_df.items():
                df.to_csv(f'gs://{BUCKET_NAME}/{PATH_TO_CERTI_DATA}{suffix}.csv', index = False)
        
        print('-'*6 , ' stuffs has been saved successfully ', '-'*6 )
    except Exception as e:
        print("!!! error in data saving to bucket !!!")
        print("!!! check required dependencies !!!")
        print("Error: ", e)

def BE_pre_check():
    try:
        global total_pages, total_responses
        response = requests.get(URL, json=METADATA, headers=HEADERS)
        total_pages = response.json()["data"]["total_page_num"]
        total_responses = response.json()["data"]["total_num"]
        print(f'Total Pages : {total_pages}')
        return total_pages, total_responses
    except Exception as e:
        print("!!!! Prechecker failed !!!! \nError: ", e)


if __name__ == "__main__":
    BE_initialise_data()
    # print(IDS)
