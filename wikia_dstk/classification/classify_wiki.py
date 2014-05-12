import sys
import numpy as np
import traceback
from collections import OrderedDict
from sklearn.cross_validation import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.datasets import make_moons, make_circles, make_classification
from sklearn.neighbors import KNeighborsClassifier
from sklearn.svm import SVC
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.lda import LDA
from sklearn.qda import QDA
from sklearn.feature_extraction.text import TfidfVectorizer


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
                                   [line.decode(u'utf8').strip().split(u',') for line in fl]])

    print u"Vectorizing..."
    vectorizer = TfidfVectorizer()
    rows_transformed = vectorizer.fit_transform(wid_to_features.values())
    wid_to_features_transformed = OrderedDict(zip(*[wid_to_features.keys(), rows_transformed]))

    names = [
        u"Nearest Neighbors",
        u"Linear SVM",
        u"RBF_SVM",
        u"Decision Tree",
        u"Random_Forest",
        u"AdaBoost",
        u"Naive Bayes",
        u"LDA",
        u"QDA"
    ]
    classifiers = [
        KNeighborsClassifier(3),
        SVC(kernel=u"linear", C=0.025),
        SVC(gamma=2, C=1),
        DecisionTreeClassifier(max_depth=999),
        RandomForestClassifier(max_depth=999, n_estimators=100, max_features=2),
        AdaBoostClassifier(),
        GaussianNB(),
        LDA(),
        QDA()
    ]

    #ints_to_classes = dict([(i, group[0]) for i, group in enumerate(groups.items())])

    data = [(str(wid), i) for i, (key, wids) in enumerate(groups.items()) for wid in wids]

    print u"Running leave-one-out cross-validation..."
    perf = {}
    for j in range(0, len(classifiers)):
        try:
            clf = classifiers[j]
            print j, len(names)
            print names[j]
            predictions = []

            for i in range(0, len(data)):
                try:
                    training, classes = zip(*[(wid_to_features[str(wid)], cls)
                                              for wid, cls in data[:i] + data[i+1:]])
                except IndexError:
                    training, classes = zip(*[(wid_to_features[str(wid)], cls)
                                              for wid, cls in data[:i]])
                clf.fit(vectorizer.transform(training).toarray(), classes)
                predictions.append(clf.predict([vectorizer.transform(wid_to_features[str(data[i][0])].toarray()[0])]))
            print predictions
            successes = len([i for i in range(0, len(data)) if data[i][1] == predictions[i]])
            print successes
            perf[names[j]] = successes
        except Exception as e:
            print e
            print traceback.format_exc()

    print perf


if __name__ == u'__main__':
    main()