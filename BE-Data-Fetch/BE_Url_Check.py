# BE Url Checker

# function to check if file at given link exists or not
import requests
import validators
from BE_utils import URL_SET 
URL_PATTERN = ['.mp4', 'diacam360', 'ds-360.jaykar', 'gem360']

def BE_check_url_for_pattern(link):
    for pattern in URL_PATTERN :       # check for different pattern in url
        if(pattern in link ):     
            return None, pattern + " found in URL"
    response = requests.get(link)
    URL_SET.add(link)
    if(response.status_code >= 200 and response.status_code <= 299):
        return response, "Response generated"
    return None, "Non responsive Link"

def BE_Check_url(link):
  if(validators.url(link) == True):     # validate url
      if(link not in URL_SET):         # check for repeatative url
        return BE_check_url_for_pattern(link)
      else:  
        return None, "Repeatative URL"

  return None, "Invalid URL format"
