from BE_Index import BE_Fetch_data
from BE_utils import initialise

def BE_main(request):
    global total_pages, total_responses, data
    total_pages, total_responses, data = initialise()
    update_overall, total_overall = BE_Fetch_data()
    return "Total Response: " + total_overall + "Updated Response: " + update_overall

