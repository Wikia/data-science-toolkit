import sys
import traceback
from collections import OrderedDict
from sklearn.neighbors import KNeighborsClassifier
from sklearn.svm import SVC
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.lda import LDA
from sklearn.qda import QDA
from sklearn.feature_extraction.text import TfidfVectorizer
from multiprocessing import Pool


def main():
    fl = open(sys.argv[1], u'r')

    groups = dict(
        comics=[2233, 631, 330278, 2446, 405961, 47003, 566447, 198946, 4385, 2237],
        tv=[130814, 18733, 1228, 26337, 1581, 971, 4095, 831, 684, 13346, 3200],
        games=[304, 3125, 300851, 208733, 490, 14764, 3035, 3510, 462, 3646, 147],
        music=[263695, 43339, 84, 22001, 710614, 2006, 79512, 4446, 113969, 203914],
        lifestyle=[11557, 283, 425, 40, 78127, 322043, 3638, 4486, 544419, 3355, 8390],
        books=[1575, 7045, 935, 114341, 15738, 694030, 12244, 265480, 12331, 379, 509, 35171, 147, 159],
        movies=[509, 35171, 147, 6294, 559, 177996, 9231, 1668, 159, 277726, 613758, 6954]
    )

    print u"Loading CSV..."
    wid_to_features = OrderedDict([(splt[0], u" ".join(splt[1:])) for splt in
                                   [line.decode(u'utf8').strip().split(u',') for line in fl]
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
        loo_class = feature_keys[i]
        del feature_rows_loo[i]
        del feature_keys_loo[i]
        loo_args.append(
            (vectorizer.transform(feature_rows),                # train
             [wid_to_class[str(wid)] for wid in feature_rows],  # classes for training set
             [vectorizer.transform([loo_row])],                 # predict
             [loo_class]                                        # expected class
             )
        )

    classifiers = {
        u"Nearest Neighbors": KNeighborsClassifier(3),
        u"Linear SVM": SVC(kernel="linear", C=0.025),
        u"RBF_SVM": SVC(gamma=2, C=1),
        u"Decision Tree": DecisionTreeClassifier(max_depth=999),
        u"Random_Forest": RandomForestClassifier(max_depth=999, n_estimators=100, max_features=2),
        u"AdaBoost": AdaBoostClassifier(),
        u"Naive Bayes": GaussianNB(),
        u"LDA": LDA(),
        u"QDA": QDA()
    }

    print u"Running leave-one-out cross-validation..."

    p = Pool(processes=8)
    print p.map_async(classify, [(i, loo_args) for i in classifiers.items()]).get()


def classify(clf_tup, loo):
    try:
        name, clf = clf_tup
        predictions = []
        expectations = []
        for i, (training, classes, predict, expected) in enumerate(loo):
            print name, i
            clf.fit(training, classes)
            predictions.append(clf.predict())
            expectations.append(expected)
        score = len([i for i in range(0, len(predictions)) if predictions[i] == expectations[i]])
        print name, score
        return name, score
    except Exception as e:
            print e
            print traceback.format_exc()


if __name__ == u'__main__':
    main()