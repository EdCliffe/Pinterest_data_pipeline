from calendar import c


#%% 
import re
def follower_count_num(count):
    count = re.sub('k','000', count)
    count = re.sub('M','000000', count)
    count = re.sub('B','000000000', count)
    return print(count)

count = "10B"
follower_count_num(count)
