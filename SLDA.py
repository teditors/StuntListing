
''' 10/24/18 1:30PM PST - version 0.20
  script to output neglected accounts, also missing data report
  requirements:
 	1. jamie_csv/ directory exists where script is run
	2. in jamie_csv/ the following 5 files exist:
		1. availability******.csv
		2. list_requests******.csv
		3. lists******.csv
		4. login******.csv
		5. stunt******.csv
	3. files have the format of the files in your email 10/23/18
'''

import pandas as pd
import datetime as dt
import glob
from functools import reduce


avail_csv = glob.glob('jamie_csv/availability******.csv')
avail = pd.read_csv(avail_csv[0], index_col='ID', 
                    names=['ID', 'idk1', 'date1', 'date2', 'idk2', 'idk3' ],
                   parse_dates=['date1', 'date2'], infer_datetime_format=True).sort_index().fillna('missing')
avail = avail.drop(columns=['idk3'])


list_req_csv = glob.glob('jamie_csv/list_requests******.csv')
list_req = pd.read_csv(list_req_csv[0], index_col='ID', 
                       names=['ID', 'idk3', 'idk4','email1', 'email2', 'passwd', 'date3' ],
                      parse_dates=['date3'], infer_datetime_format=True).sort_index().fillna('missing')


lists_csv = glob.glob('jamie_csv/lists******.csv')
lists = pd.read_csv(lists_csv[0], index_col='TID', names=['TID', 'idk5', 'team', 'date4' ]).sort_index().fillna('missing')


login_csv = glob.glob('jamie_csv/login******.csv')
login = pd.read_csv(login_csv[0], index_col='ID', 
                    names=['ID', 'first name1', 'last name1', 'email3', 'phone', 'job', 'idk6',
                           'idk7', 'idk8', 'acc type', 'idk9', 'idk10' ]).sort_index().fillna('missing')
# format phone numbers
login['phone'] = login['phone'].astype(str).apply(lambda x: ('('+x[:3]+')'+x[3:6]+'-'+x[6:10]) if x != 'missing' else 'missing')


stunt_csv = glob.glob('jamie_csv/stunt******.csv')
stunt = pd.read_csv(stunt_csv[0], delimiter=',', index_col='ID', 
                    names=['ID', 'first name2', 'last name2', 'year', 'sex', 'tags', 'hair',
                           'height', 'weight', 'idk11', 'idk12', 'idk13', 'idk14', 'idk15']).sort_index().fillna('missing')


days_ago = 180  # set this to desired neglect time
cutoff = dt.datetime.today() - dt.timedelta(days=days_ago)
neglected = avail[avail.date2 < cutoff]

neglected = neglected.merge(login, left_index=True, right_index=True, how='left').drop(columns = ['job'])
neglected.to_csv('jamie_csv/neglected_accts.csv', header=False)

data_list = [login, stunt]  # lists, list_req, avail not included, diff index
dm = reduce(lambda left,right: pd.merge(left, right, how='left', on='ID'), data_list).fillna('missing')


dm = dm.drop(columns=['idk6', 'idk7', 'first name2', 'last name2', 'idk9', 'idk10', 'idk12', 'idk13', 'idk14'] )
missing_list = dm[(dm == 'missing').any(1)]


mr = missing_list.loc[:, 'first name1':'email3' ]
mr = mr.assign(MISSING='')


for r_idx, row in missing_list.iterrows():
    s = ['']
    for c_idx, col in row.iteritems():
        if col == 'missing':
            s.append(c_idx)
    mr.at[r_idx, 'MISSING'] = ' '.join(str(i) for i in s)


mr.to_csv('jamie_csv/missing_info.csv', header=False)

