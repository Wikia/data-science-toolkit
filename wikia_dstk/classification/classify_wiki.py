import sys
import time
from collections import defaultdict
from . import vertical_labels, wid_to_class, class_to_label, Classifiers
from collections import OrderedDict
from sklearn.feature_extraction.text import TfidfVectorizer
from argparse import ArgumentParser, FileType


def get_args():
    ap = ArgumentParser()
    ap.add_argument(u'--num-processes', dest=u'num_processes', default=8)
    ap.add_argument(u'--classifier', dest=u'classifier', default=u'naive_bayes')
    ap.add_argument(u'--infile', dest=u'infile', type=FileType(u'r'), default=sys.stdin)
    ap.add_argument(u'--outfile', dest=u'outfile', type=FileType(u'w'), default=sys.stdout)
    return ap.parse_args()


def main():
    start = time.time()
    args = get_args()

    groups = vertical_labels
    print u"Loading CSV..."
    wid_to_features = OrderedDict([(splt[0], u" ".join(splt[1:])) for splt in
                                   [line.decode(u'utf8').strip().split(u',') for line in args.infile]
                                   if int(splt[0]) in [v for g in groups.values() for v in g]  # only in group for now
                                   ])

    unknowns = OrderedDict([(splt[0], u" ".join(splt[1:])) for splt in
                            [line.decode(u'utf8').strip().split(u',') for line in args.infile]
                            if int(splt[0]) not in [v for g in groups.values() for v in g]
                            ])

    print u"Vectorizing..."
    vectorizer = TfidfVectorizer()
    feature_rows = wid_to_features.values()
    feature_keys = [wid_to_class[int(key)] for key in wid_to_features.keys()]
    vectorizer.fit_transform(feature_rows)

    clf = Classifiers.get(args.classifier)
    print u"Training on %d instances..." % len(feature_rows)
    clf.fit(vectorizer.transform(feature_rows).toarray(), feature_keys)
    print u"Predicting for %d unknowns..." % len(unknowns)
    predictions = clf.predict(vectorizer.transform(unknowns.values()).toarray())

    prediction_counts = defaultdict(int)
    for p in predictions:
        prediction_counts[class_to_label[p]] += 1
    print prediction_counts

    print u"Writing to file"
    for i, wid in enumerate(unknowns.keys()):
        args.outfile.write(u",".join([wid, class_to_label[predictions[i]]])+u"\n")

    print u"Finished in", (time.time() - start), u"seconds"


if __name__ == u'__main__':
    main()