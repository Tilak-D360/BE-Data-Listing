NUM_PER_PAGE = 5000

URL = f"https://www.brilliantearth.com/v360-api/get-v360-link/?page=1&per_page={NUM_PER_PAGE}"
DATA = {
        "username": "beteam",
        "password": "Cacti17orange",
        "showPassword": "false"
}
HEADERS = {'AUTH-TOKEN': 'TElWRV9WMzYwX0xJTktfSlNPTl9UT0tFTg=='}
CATEGORIES = ('no_update', 'resync_needY', 'activeY_downloadN', 'activeN_downloadN_coloredDiamondY')

import requests
def initialise():
    global total_pages, total_responses, data
    response = requests.get(URL, json=DATA, headers=HEADERS)
    total_pages = response.json()["data"]["total_page_num"]
    total_responses = response.json()["data"]["total_num"]
    # returns response as json format with
    # code    -> code of response
    # message -> text message
    # data    -> main data
    #  ↪ curr_page_num
    #  ↪ has_next_page
    #  ↪ has_previous_page
    #  ↪ NUM_PER_PAGE
    #  ↪ total_num
    #  ↪ total_page_num
    #  ↪ v360_links -> list which contains all required data in dictionary form

#     print("Total Response : ", total_responses)
#     print("Total Page Num : ", total_pages)

    data = response.json()['data']['v360_links'][0]
    # Required data with fields
    #   ↪ certificate_number -> number
    #   ↪ v360_link -> link to file
    #   ↪ vendor_name -> string
    #   ↪ create_date -> time
    #   ↪ download_status -> 'Y' or 'N'
    #   ↪ active_status -> 'Y' or 'N'
    #   ↪ colored_diamonds -> 'Y' or 'N'
    #   ↪ resync_need -> null or 'Y' or 'N' ('Y' or 'N' in last 10000 responses) 
    return total_pages, total_responses, data
