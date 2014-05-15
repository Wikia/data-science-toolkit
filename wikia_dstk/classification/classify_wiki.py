import sys
import time
import numpy as np
import traceback
from collections import defaultdict
from . import vertical_labels, wid_to_class, class_to_label, predict_ensemble, logger
from collections import OrderedDict
from sklearn.feature_extraction.text import TfidfVectorizer
from argparse import ArgumentParser, FileType


def get_args():
    ap = ArgumentParser()
    ap.add_argument(u'--num-processes', dest=u'num_processes', default=8)
    ap.add_argument(u'--classifiers', dest=u'classifiers', default=[], action=u"append")
    ap.add_argument(u'--infile', dest=u'infile', type=FileType(u'r'), default=sys.stdin)
    ap.add_argument(u'--as-sparse', dest=u'as_sparse', default=False, action=u'store_true')
    ap.add_argument(u'--num-topicss', dest=u'num_topics', default=999)
    ap.add_argument(u'--outfile', dest=u'outfile', type=FileType(u'w'), default=sys.stdout)
    return ap.parse_args()


def main():
    start = time.time()
    args = get_args()

    groups = vertical_labels
    logger.info(u"Loading CSV...")
    lines = [line.decode(u'utf8').strip() for line in args.infile]
    if not args.as_sparse:
        wid_to_features = OrderedDict([(splt[0], u" ".join(splt[1:])) for splt in
                                       [line.split(u',') for line in lines]
                                       if int(splt[0]) in [v for g in groups.values() for v in g]  # only in group
                                       ])

        unknowns = OrderedDict([(splt[0], u" ".join(splt[1:])) for splt in
                                [line.split(u',') for line in lines]
                                if int(splt[0]) not in [v for g in groups.values() for v in g]
                                ])
        logger.info(u"Vectorizing...")
        vectorizer = TfidfVectorizer()
        feature_rows = wid_to_features.values()
        feature_keys = [wid_to_class[int(key)] for key in wid_to_features.keys()]
        vectorizer.fit_transform(feature_rows)
        training_vectors = vectorizer.transform(feature_rows).toarray()
        test_vectors = vectorizer.transform(unknowns.values()).toarray()
    else:
        training_ids = [v for g in groups.values() for v in g]
        wid_to_features = OrderedDict()
        unknowns = OrderedDict()
        for line in lines:
            splt = line.split(u',')
            wid = splt[0]
            features = [0 for _ in range(0, args.num_topics)]
            for group in splt[1:]:
                topic, feature = group.split(u'-')
                features[int(topic)] = float(feature)
            if int(wid) in training_ids:
                wid_to_features[wid] = features
            else:
                unknowns[wid] = features

        feature_rows = wid_to_features.values()
        feature_keys = [wid_to_class[int(key)] for key in wid_to_features.keys()]
        training_vectors = np.array(feature_rows)
        test_vectors = np.array(unknowns.values())

    logger.info(u"Training %d classifiers" % len(args.classifiers))
    predictions = predict_ensemble(args.classifiers, training_vectors, feature_keys, test_vectors)
    logger.info(u"Writing to file")
    for i, wid in enumerate(unknowns.keys()):
        args.outfile.write(u",".join([wid, class_to_label[predictions[i]]])+u"\n")

    logger.info(u"Finished in %.2f seconds" % (time.time() - start))


if __name__ == u'__main__':
    main()