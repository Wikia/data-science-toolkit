import numpy as np
import time
from collections import defaultdict
from scipy.spatial.distance import cdist
from multiprocessing import Pool, TimeoutError
from datetime import datetime
from argparse import ArgumentParser, FileType
from boto import connect_s3
from ..lda import harakiri


def get_args():
    ap = ArgumentParser()
    ap.add_argument('--infile', dest="infile", type=FileType('r'))
    ap.add_argument('--s3file', dest='s3file')
    ap.add_argument('--metric', dest="metric", default="cosine")
    ap.add_argument('--slice-size', dest='slice_size', default=100, type=int)
    ap.add_argument('--use-batches', dest='use_batches', action='store_true', default=False)
    ap.add_argument('--instance-batch-size', dest='instance_batch_size', type=int, default=50000)
    ap.add_argument('--instance-batch-offset', dest='instance_batch_offset', type=int, default=0)
    ap.add_argument('--recommendation-name', dest='recommendation_name', default='video')
    ap.add_argument('--num-topics', dest='num_topics', default=999, type=int)
    return ap.parse_args()


def tup_dist(tup):
    func, docid, a, b = tup
    result = cdist(a, b, func)
    return docid, result


def get_recommendations(args, docid_to_topics, callback=None):
    print "Indexing data..."
    docids, topics = zip(*docid_to_topics.items())
    values = np.array(topics)
    nonzeroes = np.nonzero(values)
    topics_to_ids = defaultdict(dict)
    ids_to_topics = defaultdict(dict)
    positions_to_topics = defaultdict(dict)
    topics_to_positions = defaultdict(dict)

    for i in range(0, len(nonzeroes[0])):
        topics_to_ids[nonzeroes[1][i]][docids[nonzeroes[0][i]]] = 1
        ids_to_topics[docids[nonzeroes[0][i]]][nonzeroes[1][i]] = 1
        topics_to_positions[nonzeroes[1][i]][nonzeroes[0][i]] = 1
        positions_to_topics[nonzeroes[0][i]][nonzeroes[1][i]] = 1

    print "Computing distances"
    p = Pool(processes=8)
    slice_size = args.slice_size
    docids_enumerated = list(enumerate(docids))
    if args.use_batches:
        start = args.instance_batch_size * args.instance_batch_offset
        docids_enumerated = docids_enumerated[start:start+args.instance_batch_size]

    docids_to_recommendations = {}
    orphaned_docid_to_rowids = {}
    orphaned_resultiterators = []

    for i in range(0, len(docids_enumerated), slice_size):
        print i,
        start = time.time()

        curr_docids = docids_enumerated[i:i+slice_size]
        shared_topic_rowids = []
        docid_to_shared_rowids = {}
        for _, did in curr_docids:
            curr_tops = ids_to_topics[did].keys()
            unique_ids = list(set([row_id for topic in curr_tops for row_id in topics_to_positions[topic].keys()]))
            docid_to_shared_rowids[did] = unique_ids
            shared_topic_rowids.append(unique_ids)

        print "Computing for", slice_size
        paramlist = []
        for local_cnt, tup in enumerate(docids_enumerated[i:i+slice_size]):
            if shared_topic_rowids[local_cnt]:
                global_cnt, docid = tup
                these_rowids = [values[x] for x in shared_topic_rowids[local_cnt]]
                paramlist.append((args.metric, docid, np.array([topics[global_cnt]]), these_rowids))

        resultiterator = p.imap_unordered(tup_dist, paramlist)
        for j in range(0, len(paramlist)):
            if j % (args.slice_size/10) == 0:
                print j, '/ 10'
            if j % 8 == 0:
                if j >= 8:
                    print (time.time() - every_8)/8, "recommendations / sec"
                every_8 = time.time()
            try:
                docid, result = resultiterator.next(60)
            except TimeoutError:
                print "Giving up on", len(docid_to_shared_rowids), "docs"
                orphaned_resultiterators.append(resultiterator)
                orphaned_docid_to_rowids.update(docid_to_shared_rowids)
                break  # waited an entire minute, screw it
            collated = sorted([(docids[rowid], result[0][k])
                               for k, rowid in enumerate(docid_to_shared_rowids[docid])], key=lambda y: y[1])[:25]
            del docid_to_shared_rowids[docid]  # we can reuse it to find out what's left
            recommended_ids = map(lambda z: z[0], collated)
            if callback:
                apply(callback, (docid, recommended_ids))
            else:
                docids_to_recommendations[docid] = recommended_ids

        mins = (time.time() - start)/60.0
        print "took", mins, "mins for", slice_size

    print len(orphaned_docid_to_rowids), "results LEFT BEHIND"

    return docids_to_recommendations


def to_csv(args, docid_to_topics):
    fname = '%s-recommendations-%s.csv' % (args.metric, str(datetime.strftime(datetime.now(), '%Y-%m-%d-%H-%M')))
    if args.use_batches:
        fname = "%d-%d-%s" % (args.instance_batch_size, args.instance_batch_offset, fname)
    print fname
    with open(fname, 'w') as fl:
        get_recommendations(args, docid_to_topics,
                            callback=lambda x, y: fl.write("%s,%s\n" % (x, ",".join(y))))
    bucket = connect_s3().get_bucket('nlp-data')
    keyname = 'recommendations/%s/%s' % (args.recommendation_name, fname)
    k = bucket.new_key(keyname)
    k.set_contents_from_filename(fname)
    print keyname


def main():
    args = get_args()
    print "Scraping CSV"
    docid_to_topics = dict()

    if args.s3file:
        fname = args.s3file.split('/')[-1]
        connect_s3().get_bucket('nlp-data').get_key(args.s3file).get_file(open(fname, 'w'))
        fl = open(fname, 'r')
    else:
        fl = args.infile

    for line in fl:
        cols = line.strip().split(',')
        docid = cols[0]
        docid_to_topics[docid] = [0] * args.num_topics  # initialize
        for col in cols[1:]:
            topic, val = col.split('-')
            docid_to_topics[docid][int(topic)] = float(val)

    to_csv(args, docid_to_topics)
    harakiri()


if __name__ == '__main__':
    main()
