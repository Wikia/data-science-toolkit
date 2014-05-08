import logging
import os
from argparse import ArgumentParser

logfile = u'/var/log/wikia_dstk.log'
log_level = logging.INFO
logger = logging.getLogger(u'wikia_dstk')
logger.setLevel(log_level)
ch = logging.StreamHandler()
ch.setLevel(log_level)
formatter = logging.Formatter(
    u'%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
ch = logging.FileHandler(logfile)
ch.setLevel(log_level)
ch.setFormatter(formatter)
logger.addHandler(ch)


def log(*args):
    logger.info(u" ".join([unicode(a) for a in args]))


def chrono_sort(directory):
    """
    Return a list of files in a directory, sorted chronologically

    :type directory: string
    :param directory: A filepath within which to sort files

    :rtype: list
    :return: A list of tuples having the format (filepath, time of last
             modification)
    """
    files = [(os.path.join(directory, filename),
              os.path.getmtime(os.path.join(directory, filename))) for filename
             in os.listdir(directory)]
    files.sort(key=lambda x: x[1])
    return files


def ensure_dir_exists(directory):
    """
    Make sure the directory given as an argument exists, and returns the same
    directory

    :type directory: string
    :param directory: A filepath to create if it doesn't already exist

    :rtype: string
    :return: The filepath originally passed as an argument
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
    return directory


def get_argparser_from_config(default_config):
    """
    Allows us to manipulate these values from the command line.
    AP instance returned so we can manipulate it in the client scripts.
    """
    ap = ArgumentParser()
    for key in default_config:
        ap.add_argument(
            '--%s' % key, type=type(default_config[key]),
            dest=key.replace('-', '_'), default=default_config[key])
    ap.set_defaults(**default_config)
    return ap


def argstring_from_namespace(namespace, unknowns=[]):
    """
    Let's us pass args to child processes from already-parsed args.
    """
    argdict = vars(namespace)
    arglist = []
    for key in argdict:
        arglist.append("--%s=%s" % (key.replace('_', '-'), str(argdict[key])))
    return " ".join(arglist + unknowns)
