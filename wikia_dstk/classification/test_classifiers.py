import traceback
import time
from . import vertical_labels, Classifiers
from collections import OrderedDict, defaultdict
from sklearn.feature_extraction.text import TfidfVectorizer
from multiprocessing import Pool
from argparse import ArgumentParser, FileType


def get_args():
    ap = ArgumentParser()
    ap.add_argument('--class-file', type=FileType('r'), dest='class_file')
    ap.add_argument('--features-file', type=FileType('r'), dest='features_file')
    return ap.parse_args()


def main():
    args = get_args()

    if args.class_file:
        groups = defaultdict(list)
        for line in args.class_file:
            splt = line.strip().split(',')
            groups[splt[1]].append(int(splt[0]))
    else:
        groups = vertical_labels
    print u"Loading CSV..."
    wid_to_features = OrderedDict([(splt[0], u" ".join(splt[1:])) for splt in
                                   [line.decode(u'utf8').strip().split(u',') for line in args.features_file]
                                   if int(splt[0]) in [v for g in groups.values() for v in g]  # only in group for now
                                   ])

    print u"Vectorizing..."
    vectorizer = TfidfVectorizer()
    feature_rows = wid_to_features.values()
    feature_keys = wid_to_features.keys()
    vectorizer.fit_transform(feature_rows)

    loo_args = []

    print u"Prepping leave-one-out data set..."
    data = [(str(wid), i) for i, (key, wids) in enumerate(groups.items()) for wid in wids]
    wid_to_class = dict(data)
    for i in range(0, len(feature_rows)):
        feature_keys_loo = [k for k in feature_keys]
        feature_rows_loo = [f for f in feature_rows]
        loo_row = feature_rows[i]
        loo_class = wid_to_class[str(feature_keys[i])]
        del feature_rows_loo[i]
        del feature_keys_loo[i]
        loo_args.append(
            (vectorizer.transform(feature_rows),                # train
             [wid_to_class[str(wid)] for wid in feature_keys],  # classes for training set
             vectorizer.transform([loo_row]),                   # predict
             [loo_class]                                        # expected class
             )
        )

    print u"Running leave-one-out cross-validation..."

    p = Pool(processes=8)
    print p.map_async(classify, [((name, clf), loo_args) for name, clf in Classifiers.each_with_name()]).get()


def classify(arg_tup):
    start = time.time()
    try:
        (name, clf), loo = arg_tup
        predictions = []
        expectations = []
        for i, (training, classes, predict, expected) in enumerate(loo):
            print name, i
            clf.fit(training.toarray(), classes)
            predictions.append(clf.predict(predict.toarray()))
            expectations.append(expected)
        score = len([i for i in range(0, len(predictions)) if predictions[i] == expectations[i]])
        print name, score, time.time() - start
        return name, score, time.time() - start
    except Exception as e:
            print e
            print traceback.format_exc()


if __name__ == u'__main__':
    main()