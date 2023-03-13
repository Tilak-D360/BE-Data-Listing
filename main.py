# version 2.3
import functions_framework
import pandas as pd

from be_worker import BE_Fetch_data, BE_update_dfs, BE_initialise_vars
from be_utils import BE_initialise_data, BE_save_data, BE_pre_check, total_pages
@functions_framework.http
def be_main(req):
    if req:
        req_json = req.get_json(silent = True)
    out = ""
    total_pages, total_responses = BE_pre_check()

    if req and req_json and ('todo' in req_json): # in case of want to reset existing files and folders in bucket pass reset in request
        LOGS, IDS, DATA = BE_initialise_data(is_reset = req_json['todo'] == 'reset')
        BE_initialise_vars(DATA)
        update_overall, total_overall = BE_Fetch_data(total_pages, req_json['todo'] == 'reset')
        print(f'total: {total_overall}, update: {update_overall}')

        LOGS, IDS, DATA = BE_update_dfs(LOGS, IDS, DATA, req_json['todo'] == 'reset')
        print(LOGS.loc[LOGS['last_check'] == 'Y'])

    else:
        LOGS, IDS, DATA = BE_initialise_data()
        BE_initialise_vars(DATA)
        update_overall, total_overall = BE_Fetch_data(total_pages)
        print(f'total: {total_overall}, update: {update_overall}')

        LOGS, IDS, DATA = BE_update_dfs(LOGS, IDS, DATA)
        print(LOGS.loc[LOGS['last_check'] == 'Y'])
        
    
    if req and req_json and 'todo' in req_json:
        BE_save_data(LOGS, IDS, DATA, req_json['todo'] == 'flag')
        out = 'Todo: ' + req_json['todo']
    else:
        BE_save_data(LOGS, IDS, DATA)
    
    return out + "Total Response: " + str(total_overall) + " Updated Response: " + str(update_overall)
    
@functions_framework.http
def be_trial(req):
    pass

def be_reset_trial(startPage = 1, do_flag = False):
    out = ""
    total_pages, total_responses = BE_pre_check()
    LOGS, IDS, DATA = BE_initialise_data(is_reset = True)
    BE_initialise_vars(DATA)
    update_overall, total_overall = BE_Fetch_data(total_pages, start = startPage, is_reset = True)
    print(f'total: {total_overall}, update: {update_overall}')

    LOGS, IDS, DATA = BE_update_dfs(LOGS, IDS, DATA, is_reset = True)
    print(LOGS.loc[LOGS['last_check'] == 'Y'])
    BE_save_data(LOGS, IDS, DATA, do_flag)
    
    return out + "Total Response: " + str(total_overall) + " Updated Response: " + str(update_overall)

def be_flag_trial():
    out = ""
    total_pages, total_responses = BE_pre_check()
    LOGS, IDS, DATA = BE_initialise_data(is_reset = False)
    BE_initialise_vars(DATA)
    update_overall, total_overall = BE_Fetch_data(total_pages, is_reset = False)
    print(f'total: {total_overall}, update: {update_overall}')

    LOGS, IDS, DATA = BE_update_dfs(LOGS, IDS, DATA, is_reset = False)
    print(LOGS.loc[LOGS['last_check'] == 'Y'])
    BE_save_data(LOGS, IDS, DATA, is_flag = True)
    
    return out + "Total Response: " + str(total_overall) + " Updated Response: " + str(update_overall)

def be_regular_trial(startIndex = 1, do_flag = False):
    out = ""
    total_pages, total_responses = BE_pre_check()
    LOGS, IDS, DATA = BE_initialise_data(is_reset = False)
    BE_initialise_vars(DATA)
    update_overall, total_overall = BE_Fetch_data(total_pages, startIndex, False)
    print(f'total: {total_overall}, update: {update_overall}')

    LOGS, IDS, DATA = BE_update_dfs(LOGS, IDS, DATA, is_reset = False)
    print(LOGS.loc[LOGS['last_check'] == 'Y'])
    BE_save_data(LOGS, IDS, DATA, is_flag = do_flag)
    
    return out + "Total Response: " + str(total_overall) + " Updated Response: " + str(update_overall)

if __name__ == "__main__":
    # be_reset_trial(80, do_flag = True)
    # be_flag_trial()
    be_regular_trial(80, True)