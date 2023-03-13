# BE_Condition

from be_utils import CATEGORIES 

def BE_filter(data):
  if(data["resync_need"] == 'Y'):
    return CATEGORIES[1]
  
  if(data["download_status"] == 'N' and data["active_status"] == 'Y'):
    return CATEGORIES[2]
  
  if(data["download_status"] == 'N' and data["active_status"] == 'N' and data['colored_diamonds'] == 'Y'):
    return CATEGORIES[3]
  
  return CATEGORIES[0]