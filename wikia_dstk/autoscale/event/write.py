from __future__ import division
from datetime import datetime, timedelta

LAST_INDEXED = '/data/last_indexed.txt'
SPLIT = 4

def total_seconds(td):
    """Return the total number of seconds in a datetime.timedelta object"""
    return (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / 10**6

with open(LAST_INDEXED, 'r') as f:
    #last_indexed = datetime.isoformat(f.read())
    last_indexed = datetime.strptime(f.read().strip(), '%Y-%m-%dT%H:%M:%S.%f')

now = datetime.utcnow()

delta = total_seconds(now - last_indexed)
increment = delta / SPLIT
#delimiters = []
#for i in range(SPLIT):
#    delimiters.append(last_indexed + timedelta(seconds=increment*i))

delimiters = [(last_indexed + timedelta(seconds=increment*i)) for i in range(SPLIT)]
delimiters.append(now)

#print '\n'.join([datetime.isoformat(delimiter) for delimiter in delimiters])

with open(LAST_INDEXED, 'w') as f:
    f.write(datetime.isoformat(now))

for i in range(SPLIT):
    query = 'iscontent:true AND lang:en AND wam:[50 TO *] AND indexed:[%sZ TO %sZ]' % (datetime.isoformat(delimiters[i]), datetime.isoformat(delimiters[i+1]))
    print query




#import sys
#
#input_file = sys.argv[1]
#
#events_dir = '/data/events/'
#
#for wid in open(input_file):
#    wid = wid.strip()
#    print 'writing', wid
#    query = 'wid:%s AND touched:[2013-09-30T23:59:59.999Z TO *] AND iscontent:true' % wid
#    event_file = events_dir + wid
#    with open(event_file, 'w') as f:
#        f.write(query)
