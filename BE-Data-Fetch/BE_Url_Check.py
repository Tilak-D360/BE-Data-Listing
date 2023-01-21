# BE_Url_Checker

# function to check if file at given link exists or not
import requests
import validators
from BE_utils import URL_SET 
URL_PATTERN = ['.mp4', 'diacam360', 'ds-360.jaykar', 'gem360']

def BE_check_url_for_pattern(link):
    for pattern in URL_PATTERN :       # check for different pattern in url
        if(pattern in link ):     
            return None, pattern + " found in URL"
    # response = requests.get(link)
    # if(response.status_code >= 200 and response.status_code <= 299):
        # return None, "Response generated"
    return None, "Link"

def BE_Check_url(link):
  if(link not in URL_SET):         # check for repeatative url
    URL_SET.add(link)
    if(validators.url(link) == True):     # validate url
      return BE_check_url_for_pattern(link)
    else:
      return None, "Invalid URL format"
  return None, "Repeatative URL"
      
