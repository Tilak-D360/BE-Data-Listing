# BE Url Checker

# function to check if file at given link exists or not
import requests
import validators

def BE_Check_url(link):
  if(validators.url(link) == True):
    # response = requests.get(link)
    # if(response.status_code >= 200 and response.status_code <= 299):
    #   return response, 0
    return None, 0
  return None, 1
