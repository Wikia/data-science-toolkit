from __future__ import division
from datetime import datetime, timedelta

# This script writes a Solr query for all documents indexed since last time,
# split into multiple event files to facilitate multiprocessing

LAST_INDEXED = '/data/last_indexed.txt'
SPLIT = 4  # Number of event files to split the time delta into


def total_seconds(td):
    """
    Return the total number of seconds in a datetime.timedelta object
    Explicitly defined, since official implementation is missing from
    datetime.timedelta before Python 2.7

    :type td: datetime.timedelta
    :param td: A timedelta object for which to calculate # of seconds

    :rtype: float
    :return: The number of seconds in the timedelta object
    """
    return ((td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) /
            10**6)

# Read the date this script was last run
with open(LAST_INDEXED, 'r') as f:
    last_indexed = datetime.strptime(f.read().strip(), '%Y-%m-%dT%H:%M:%S.%f')

# Set the current date
now = datetime.utcnow()

# Split the time delta into equal parts
delta = total_seconds(now - last_indexed)
increment = delta / SPLIT
delimiters = [(last_indexed + timedelta(seconds=increment*i)) for i in
              range(SPLIT)] + [now]

# Write the current date to file for future use
with open(LAST_INDEXED, 'w') as f:
    f.write(datetime.isoformat(now))

# Write multiple Solr queries to the events directory
for i in range(SPLIT):
    from_ = datetime.isoformat(delimiters[i])
    to = datetime.isoformat(delimiters[i+1])
    query = 'iscontent:true AND lang:en AND indexed:[%sZ TO %sZ]' % (from_, to)
    with open('/data/events/%d' % i, 'w') as f:
        f.write(query)
