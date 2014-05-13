import sys
import time
import numpy as np
import traceback
from collections import defaultdict
from . import vertical_labels, wid_to_class, class_to_label, Classifiers
from collections import OrderedDict
from sklearn.feature_extraction.text import TfidfVectorizer
from argparse import ArgumentParser, FileType


def get_args():
    ap = ArgumentParser()
    ap.add_argument(u'--num-processes', dest=u'num_processes', default=8)
    ap.add_argument(u'--classifiers', dest=u'classifiers', default=[], action=u"append")
    ap.add_argument(u'--infile', dest=u'infile', type=FileType(u'r'), default=sys.stdin)
    ap.add_argument(u'--outfile', dest=u'outfile', type=FileType(u'w'), default=sys.stdout)
    return ap.parse_args()


def main():
    start = time.time()
    args = get_args()

    groups = vertical_labels
    print u"Loading CSV..."
    lines = [line.decode(u'utf8').strip() for line in args.infile]
    wid_to_features = OrderedDict([(splt[0], u" ".join(splt[1:])) for splt in
                                   [line.split(u',') for line in lines]
                                   if int(splt[0]) in [v for g in groups.values() for v in g]  # only in group for now
                                   ])

    unknowns = OrderedDict([(splt[0], u" ".join(splt[1:])) for splt in
                            [line.split(u',') for line in lines]
                            if int(splt[0]) not in [v for g in groups.values() for v in g]
                            ])

    print u"Vectorizing..."
    vectorizer = TfidfVectorizer()
    feature_rows = wid_to_features.values()
    feature_keys = [wid_to_class[int(key)] for key in wid_to_features.keys()]
    vectorizer.fit_transform(feature_rows)
    scores = defaultdict(defaultdict(list))
    print u"Training", len(args.classifiers), u"classifiers"
    for classifier_string in args.classifiers:
        clf = Classifiers.get(classifier_string)
        classifier_name = Classifiers.classifier_keys_to_names[classifier_string]
        print u"Training a %s classifier on %d instances..." % (classifier_name, len(feature_rows))
        clf.fit(vectorizer.transform(feature_rows).toarray(), feature_keys)
        print u"Predicting for %d unknowns..." % len(unknowns)
        vectors = vectorizer.transform(unknowns.values()).toarray()
        for i, v in enumerate(vectors):
            print u"Predicting probability for", v
            try:
                clf.predict_proba(v)
            except (SystemExit, SystemError, Exception) as e:
                print e
                print traceback.format_exc()

        prediction_probabilities = [clf.predict_proba(v) for v in vectors]
        print u"I got here"
        print prediction_probabilities
        print u"And I got here"
        prediction_counts = defaultdict(int)
        for i, p in enumerate(prediction_probabilities):
            prediction_counts[class_to_label[p.index(max(p))]] += 1
            scores[i][classifier_string].append(p)
        print u"And I also got here"
        print classifier_string, prediction_counts

    print u"Interpolating Predictions"
    prediction_counts = defaultdict(int)
    predictions = []
    for i in scores:
        summed = np.sum(scores[i].values(), axis=0) / float(len(scores[i]))
        predictions.append(summed.index(max(summed)))
        prediction_counts[class_to_label[predictions[-1]]] += 1
    print prediction_counts

    print u"Writing to file"
    for i, wid in enumerate(unknowns.keys()):
        args.outfile.write(u",".join([wid, class_to_label[predictions[i]]])+u"\n")

    print u"Finished in", (time.time() - start), u"seconds"


if __name__ == u'__main__':
    main()