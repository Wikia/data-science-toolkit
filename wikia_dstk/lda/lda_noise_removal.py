# Rewrite a sparse topics CSV file such that topic features appearing in more
# wikis than a given limit have the frequency 'null'. This signals to Solr that
# the topic features in question are to be deleted.

import sys
from collections import defaultdict
from optparse import OptionParser

parser = OptionParser()
parser.add_option('-i', '--input', dest='input_file', action='store', help='Location of CSV file from which to remove high-frequency features')
parser.add_option('-o', '--output', dest='output_file', action='store', default=False, help='Location of CSV file to write to')
parser.add_option('-m', '--max-freq', dest='maxfreq', action='store', default=500, help='Integer frequency above which a feature will be omitted')
parser.add_option('-t', '--topics', dest='topics', action='store', default=999, help='Number of topics')
options, args = parser.parse_args()

if not options.output_file:
    options.output_file = 'clean-' + options.input_file

tally = defaultdict(int)

with open(options.input_file) as input_file:
    for line in input_file:
        data = line.strip().split(',')
        if len(data) > 1:
            for pair in data[1:]:
                feature, frequency = pair.split('-')
                tally[int(feature)] += 1

with open(options.input_file) as input_file:
    with open(options.output_file, 'w') as output_file:
        for line in input_file:
            data = line.strip().split(',')
            pairs = dict([(int(pair.split('-')[0]), float(pair.split('-')[1])) for pair in data[1:]])
            output = ','.join([data[0]] + ['%d-%.8f' % (n, pairs.get(n, 0)) if tally[n] < options.maxfreq else '%d-%.8f' % (n, 0) for n in range(options.topics)]) + '\n'
            output_file.write(output)
