# function to check url
import requests
import validators
from urllib.parse import urlparse

global URL_PATTERN, BAD_DOMAIN, URL_SET
URL_PATTERN = set(['.mp4', 'diacam360', 'ds-360.jaykar', 'gem360', 'vidpicture'])
BAD_DOMAIN = set()
URL_SET = set()

def BE_check_url_for_bad_domain(link):
  url = urlparse(link)
  hostname = '{uri.netloc}'.format(uri=url)
  if(hostname in BAD_DOMAIN):
    return 'bad domain: ' + hostname
  return 'link'

def BE_check_url_for_pattern(link):
    for pattern in URL_PATTERN :       # check for different pattern in url
      if(pattern in link ):     
        return 'pattern: '+pattern
    return "link"

def BE_check_url_for_repeatation(link):
  for url in URL_SET:
    if(url == link):
      return 'repeatative url'
  URL_SET.add(link)
  return 'link'

def BE_validate_url(link):   # checks for url validity, repeatation and pattern
  if(validators.url(link) == True):     # validate url
    return 'link'
  return "invalid url format"

def BE_url_res_checker(link):
  try:
    out = BE_validate_url(link) 
    if(out == 'link'):

      out = BE_check_url_for_repeatation(link)
      if(out == 'link'):
        
        out = BE_check_url_for_pattern(link)
        if(out == 'link'):
          
          out = BE_check_url_for_bad_domain(link)
          res = requests.get(link, timeout = 5)
          if(res.status_code >= 200 and res.status_code <= 299):
            return 'valid link'  
          return ('', out)[out != 'link'] + ', non responsive link'
        return out
    return out


  except requests.Timeout as err:
    url = urlparse(link)
    hostname = '{uri.netloc}'.format(uri=url)
    BAD_DOMAIN.add(hostname)
    return 'time out error link'
  except Exception as err:
    url = urlparse(link)
    hostname = '{uri.netloc}'.format(uri=url)
    BAD_DOMAIN.add(hostname)
    return 'exceptive link' 