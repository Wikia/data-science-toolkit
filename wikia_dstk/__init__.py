import os
from boto.ec2 import connect_to_region
from collections import defaultdict
from itertools import chain
from multiprocessing import Pool
from time import sleep


def chrono_sort(directory):
    """Return a list of files in a directory, sorted chronologically"""
    files = [(os.path.join(directory, filename), os.path.getmtime(os.path.join(directory, filename))) for filename in os.listdir(directory)]
    files.sort(key=lambda x: x[1])
    return files

def ensure_dir_exists(directory):
    """
    Makes sure the directory given as an argument exists, and returns the same
    directory.
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
    return directory

def run_instances_lb(ids, callable, num_instances, user_data, options=None, ami="ami-2eb7da1e"):
    """
    Run a set of instances that evenly distributes the workload between target IDs

    :type ids: list
    :param ids: A list of IDs against which to run this process

    :type callable: function
    :param callable: A function that, given an ID, returns a value that should be load-balanced

    :type num_instances: int
    :param num_instances: The number of instances to create

    :type user_data: string
    :param user_data: The script to run on instantiation, i.e. where the logic
                      goes. Should contain '%s' to pass comma-separated list of IDs as an
                      argument via a string-formatting operation

    :type options: dict
    :param options: Launch configuration options with which to instantiate an EC2Connection object

    :type ami: string
    :param ami: The AMI ID of the image to load

    :rtype: list
    :return: A list of IDs of the instances created
    """
    # Connect to EC2
    if options is None:
        options = {'ami': ami}
    conn = EC2Connection(options)

    # Split IDs into buckets of approx equal total 'callable' value
    ids = sorted(ids, key=lambda x: callable(x), reverse=True) # Sort descending
    parts = defaultdict(list) # {instance #: list of IDs to pass to instance}
    for i in range(0, len(ids), num_instances):
        for n, id_ in enumerate(ids[i:i+num_instances]):
            parts[n].append(id_)

    # Format user_data script with comma-separated list of IDs, and launch instances
    return conn.add_instances_async(num_instances, [user_data % ','.join([str(id_) for id_ in ids]) for ids in parts.values()])

class EC2Connection(object):
    """A connection to a specified EC2 region."""

    def __init__(self, options):
        """
        Instantiate a boto.ec2.connection.EC2Connection object upon which to
        perform actions.

        :type options: dict
        :param options: A dictionary containing the autoscaling options
        """
        self.region = options.get('region', 'us-west-2')
        self.price = options.get('price', '0.300')
        self.ami = options.get('ami', 'ami-2eb7da1e')
        self.key = options.get('key', 'data-extraction')
        self.sec = options.get('sec', 'sshable')
        if not isinstance(self.sec, list):
            self.sec = self.sec.split(',')
        self.type = options.get('type', 'm2.4xlarge')
        self.tag = options.get('tag', self.ami)
        self.threshold = options.get('threshold', 50)
        self.max_size = options.get('max_size', 5)
        self.conn = connect_to_region(self.region)

    def add_instances(self, count, user_data=None):
        """
        Add a specified number of instances with the same launch specification.

        :type count: int
        :param count: The number of instances to add

        :type user_data: string
        :param user_data: A script to pass to the launched instance

        :rtype: list
        :return: A list of IDs corresponding to the instances launched
        """
        # Request spot instances
        reservation = self.conn.request_spot_instances(price=self.price,
                                                       image_id=self.ami,
                                                       count=count,
                                                       key_name=self.key,
                                                       security_groups=self.sec,
                                                       user_data=user_data,
                                                       instance_type=self.type)

        # Get instance IDs for the reservation
        r_ids = [request.id for request in reservation]
        while True: # Because the requests are fulfilled independently
            sleep(5)
            requests = self.conn.get_all_spot_instance_requests(request_ids=r_ids)
            instance_ids = []
            for request in requests:
                instance_id = request.instance_id
                if instance_id is None:
                    break
                instance_ids.append(instance_id)
            if len(instance_ids) < len(reservation):
                print 'Waiting for %d instances to launch...' % len(reservation)
                continue
            break

        # Tag instances after they have launched
        tags = {'Name': self.tag}
        self.conn.create_tags(instance_ids, tags)

        return instance_ids

    def add_instances_async(self, count, user_data_scripts):
        """
        Add a specified number of instances asynchronously, each with unique user_data.

        :type count: int
        :param count: The number of instances to add

        :type user_data_scripts: list
        :param user_data_scripts: A list of strings representing the scripts to
                                  run on the individual instances

        :rtype: list
        :return: A list of IDs corresponding to the instances launched
        """
        mapped = Pool(processes=count).map_async(lambda x: self.add_instances(1, x),
                                                 user_data_scripts)
        return list(chain.from_iterable(mapped))

    def terminate(self, instance_ids):
        """
        Terminate instances with the specified IDs.

        :type instance_ids: list
        :param instance_ids: A list of strings representing the instance IDs to
                             be terminated
        """
        self.conn.terminate_instances(instance_ids)

    def get_tagged_instances(self, tag=None):
        """
        Get pending or running instances labeled with the tags specified in the
        options.

        :type tag: string
        :param tag: A string representing the tag name to operate over

        :rtype: list
        :return: A list of strings representing the IDs of the tagged instances
        """
        if tag is None:
            tag = self.tag
        filters = {'tag:Name': tag}
        return [instance.id for reservation in
                self.conn.get_all_instances(filters=filters) for instance in
                reservation.instances if instance.state_code < 32]
