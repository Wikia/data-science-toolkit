import logging
import numpy as np
from sklearn.neighbors import KNeighborsClassifier
from sklearn.svm import SVC
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.lda import LDA
from sklearn.qda import QDA
from sklearn.linear_model import LogisticRegression
from collections import OrderedDict, defaultdict


log_level = logging.INFO
logger = logging.getLogger(u'wikia_dstk.classification')
logger.setLevel(log_level)
ch = logging.StreamHandler()
ch.setLevel(log_level)
formatter = logging.Formatter(u'%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

vertical_labels = OrderedDict(
    comics=[2233, 631, 330278, 2446, 405961, 47003, 566447, 198946, 4385, 2237],
    tv=[130814, 18733, 1228, 26337, 1581, 971, 4095, 831, 684, 13346, 3200],
    games=[304, 3125, 300851, 208733, 490, 14764, 3035, 3510, 462, 3646, 147],
    music=[263695, 43339, 84, 22001, 710614, 2006, 79512, 4446, 113969, 203914],
    lifestyle=[11557, 283, 425, 40, 78127, 322043, 3638, 4486, 544419, 3355, 8390],
    books=[1575, 7045, 935, 114341, 15738, 694030, 12244, 265480, 12331, 379, 509, 35171, 147, 159],
    movies=[509, 35171, 147, 6294, 559, 177996, 9231, 1668, 159, 277726, 613758, 6954]
)

wid_to_class = dict([(wid, idx) for idx, (label, wids) in enumerate(vertical_labels.items()) for wid in wids])
class_to_label = dict([(idx, label) for idx, (label, wids) in enumerate(vertical_labels.items())])


def predict_ensemble(classifier_strings, training_vectors, vector_classes, test_vectors):
    """
    Performs ensemble prediction for a set of classifiers listed by key name in Classifiers class
    :param classifier_strings: a non-empty list of classifier keys
    :type classifier_strings: list
    :param training_vectors: a numpy array of training vectors
    :type training_vectors:class:`numpy.array`
    :param vector_classes: a list of numeric class ids for each vector, in order
    :type vector_classes: list
    :param test_vectors: a numpy array of vectors to predict class for
    :type test_vectors:class:`numpy.array`
    :return: an ordered list of classes for each test vector row
    :rtype: list
    """
    scores = defaultdict(lambda: defaultdict(list))
    for classifier_string in classifier_strings:
        clf = Classifiers.get(classifier_string)
        classifier_name = Classifiers.classifier_keys_to_names[classifier_string]

        logger.info(u"Training a %s classifier on %d instances..." % (classifier_name, len(training_vectors)))
        clf.fit(training_vectors, vector_classes)
        logger.info(u"Predicting with %s for %d unknowns..." % (classifier_name, len(test_vectors)))
        prediction_probabilities = clf.predict_proba(test_vectors)
        prediction_counts = defaultdict(int)
        for i, p in enumerate(prediction_probabilities):
            prediction_counts[class_to_label[list(p).index(max(p))]] += 1
            scores[i][classifier_string].append(p)
        logger.info((classifier_string, prediction_counts))

    logger.info(u"%s Predictions" % (u"Finalizing" if len(classifier_strings) == 1 else u"Interpolating"))
    prediction_counts = defaultdict(int)
    predictions = []
    for i in scores:
        combined = (np.sum(scores[i].values(), axis=0) / float(len(scores[i])))[0]
        predictions.append(list(combined).index(max(combined)))
        prediction_counts[class_to_label[predictions[-1]]] += 1
    logger.info(prediction_counts)
    return predictions


class Classifiers():
    """
    Let's us be dynamic from the command line on which classifer we want to use
    """
    classifiers = {
        u"Nearest Neighbors": (KNeighborsClassifier, [3], dict()),
        u"Linear SVM": (SVC, [], dict(kernel="linear", C=0.025, probability=True)),
        u"RBF_SVM": (SVC, [], dict(gamma=2, C=1)),
        u"Decision Tree": (DecisionTreeClassifier, [], dict(max_depth=999)),
        u"Random_Forest": (RandomForestClassifier, [], dict(max_depth=999, n_estimators=100, max_features=7)),
        u"AdaBoost": (AdaBoostClassifier, [], {}),
        u"Naive Bayes": (GaussianNB, [], {}),
        u"LDA": (LDA, [], {}),
        u"QDA": (QDA, [], {}),
        u"Maximum Entropy": (LogisticRegression, [], {}),
    }

    classifier_keys_to_names = {
        u"knn": u"Nearest Neighbors",
        u"linear_svm": u"Linear SVM",
        u"rbf_svm": u"RBF_SVM",
        u"decision_tree": u"Decision Tree",
        u"random_forest": u"Random_Forest",
        u"adaboost": u"AdaBoost",
        u"naive_bayes": u"Naive Bayes",
        u"lda": u"LDA",
        u"qda": u"QDA",
        u"maxent": u"Maximum Entropy",
    }

    @classmethod
    def get(cls, val):
        """
        Powers lazy instantiation of a class
        """
        if val not in cls.classifier_keys_to_names:
            raise KeyError(u"Classifier not configured for %s" % val)

        return apply(apply, cls.classifiers[cls.classifier_keys_to_names[val]])

    def __getattr__(self, item):
        return self.get(item)

    def __init__(self):
        pass

    @classmethod
    def each(cls):
        for key in cls.classifier_keys_to_names:
            yield cls.get(key)

    @classmethod
    def each_with_name(cls):
        for key in cls.classifier_keys_to_names:
            yield cls.classifier_keys_to_names[key], cls.get(key)
