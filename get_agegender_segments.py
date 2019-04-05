import datetime
import sys

def get_age_range_gender(agerange, gencode):

    if agerange == 'NULL' or agerange == '':
        if gencode == 1:
          return '|35513'
        elif gencode == 2:
          return '|35514'
        else:
          return 'NULL'

    if agerange=='18-19':
        if gencode == 2: retval='35502|35514'
        elif gencode == 1: retval='35502|35513'
        else: retval='35502|'
        return retval
    elif agerange== '20-24':
        if gencode == 2: retval='35503|35514'
        elif gencode == 1: retval='35503|35513'
        else: retval= '35503|'
        return retval
    elif agerange== '25-29':
        if gencode == 2: retval='35504|35514'
        elif gencode == 1: retval='35504|35513'
        else: retval= '35504|'
        return retval
    elif agerange== '30-34':
        if gencode == 2: retval='35505|35514'
        elif gencode == 1: retval='35505|35513'
        else: retval= '35505|'
        return retval
    elif agerange== '35-39':
        if gencode == 2: retval='35506|35514'
        elif gencode == 1: retval='35506|35513'
        else: retval= '35506|'
        return retval
    elif agerange== '40-44':
        if gencode == 2: retval='35507|35514'
        elif gencode == 1: retval='35507|35513'
        else: retval= '35507|'
        return retval
    elif agerange== '45-49':
        if gencode == 2: retval='35508|35514'
        elif gencode == 1: retval='35508|35513'
        else: retval= '35508|'
        return retval
    elif agerange== '50-54':
        if gencode == 2: retval='35509|35514'
        elif gencode == 1: retval='35509|35513'
        else: retval= '35509|'
        return retval
    elif agerange== '55-59':
        if gencode == 2: retval='35510|35514'
        elif gencode == 1: retval='35510|35513'
        else: retval= '35510|'
        return retval
    elif agerange== '60-64':
        if gencode == 2: retval='35511|35514'
        elif gencode == 1: retval='35511|35513'
        else: retval= '35511|'
        return retval
    elif agerange== '65+':
        if gencode == 2: retval='35512|35514'
        elif gencode == 1: retval='35512|35513'
        else: retval= '35512|'
        return retval
    else:
        return ''

for line in sys.stdin:

    if line != '':
        device_id,pid,e1,agerange,gender = line.split('\t')
        if len(agerange)<2: agerange=""
        if len(gender)<2: gender=""
        else: gender = gender[0:len(gender)-1]
        if agerange != "" or gender != "":
          if gender == "Male": 
            age_gender = get_age_range_gender(agerange,1)
          elif gender == "Female":
            age_gender = get_age_range_gender(agerange,2)
          else:
            age_gender = get_age_range_gender(agerange,3)
        else:
          age_gender=''
	print ('\t'.join([device_id, pid,e1,age_gender]))
