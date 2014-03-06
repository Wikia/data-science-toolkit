import os


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
