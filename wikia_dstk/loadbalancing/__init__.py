from boto.ec2 import connect_to_region
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from collections import defaultdict
from multiprocessing import Pool
from time import sleep
from uuid import uuid4


def get_instance_ids_from_reservation(conn, reservation):
    """

    Get instance IDs from a reservation

    :type conn: class:`boto.ec2.ec2connection`
    :paramm conn: an EC2 connection

    :type reservation: class:`boto.ec2.spotinstancerequest.SpotInstanceRequest`
    :param reservation: a spot instance request reservation

    :return: a list of EC2 instance IDs
    """

    # Get instance IDs for the reservation
    r_ids = [request.id for request in reservation]
    while True:  # Because the requests are fulfilled independently
        sleep(15)
        requests = conn.get_all_spot_instance_requests(request_ids=r_ids)
        instance_ids = [request.instance_id for request in requests if request.instance_id]
        if len(instance_ids) == len(r_ids):
            return instance_ids


def run_instances_lb(ids, callback, num_instances, user_data, options=None,
                     ami="ami-dc0c63ec"):
    """
    Run a set of instances that evenly distributes the workload between target
    IDs

    :type ids: list
    :param ids: A list of IDs against which to run this process

    :type callback: function
    :param callback: A function that, given an ID, returns a value that should
                     be load-balanced

    :type num_instances: int
    :param num_instances: The number of instances to create

    :type user_data: string
    :param user_data: The script to run on instantiation, i.e. where the logic
                      goes. Should contain '%s' to pass comma-separated list of
                      IDs as an argument via a string-formatting operation

    :type options: dict
    :param options: Launch configuration options with which to instantiate an
                    EC2Connection object

    :type ami: string
    :param ami: The AMI ID of the image to load

    :rtype: list
    :return: A list of IDs of the instances created
    """
    # Connect to EC2
    if options is None:
        options = {'ami': ami}
    conn = EC2Connection(options)

    # TODO: Explore diff methods of combinatorial optimization to improve this
    # Split IDs into buckets of approx equal total 'callable' value
    ids = sorted(ids, key=callback, reverse=True)  # Sort desc
    parts = defaultdict(list)  # {instance #: list of IDs to pass to instance}
    for i in range(0, len(ids), num_instances):
        for n, id_ in enumerate(ids[i:i+num_instances]):
            parts[n].append(id_)

    # Write event files containing IDs to S3 & populate a list w/ their paths
    scripts = []
    bucket = S3Connection().get_bucket('nlp-data')
    k = Key(bucket)
    for wids in parts.values():
        k.key = 'lb_events/%s' % str(uuid4())
        k.set_contents_from_string(','.join([str(wid) for wid in wids]))
        scripts.append(user_data % k.key)

    # Launch instances
    return conn.add_instances_async(scripts)


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

    def get_reservation(self, count, user_data=None):
        """
        Request a reservation from

        :type count: int
        :param count: The number of instances to add

        :type user_data: string
        :param user_data: A script to pass to the launched instance

        :rtype: class:`boto.ec2.spotinstancerequest.SpotInstanceRequest`
        :return: A spot instance request
        """
        return self.conn.request_spot_instances(price=self.price, image_id=self.ami, count=count,
                                                key_name=self.key, security_groups=self.sec, user_data=user_data,
                                                instance_type=self.type)

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
        reservation = self.get_reservation(count, user_data=user_data)
        instance_ids = self.get_instance_ids_from_reservation(self.conn, reservation)
        self.conn.create_tags(instance_ids, {'Name': self.tag})
        return instance_ids

    def add_instances_async(self, user_data_scripts, num_instances=1,  processes=2):
        """
        Add a specified number of instances asynchronously, each with unique
        user_data.

        :type user_data_scripts: iterable
        :param user_data_scripts: strings representing the scripts to
                                  run on the individual instances


        :type num_instances: int
        :param num_instances: The number of instances to spawn PER USER DATA SCRIPT

        :type processes: int
        :param processes: The number of processes to use

        :rtype:
        :return:`multiprocessing.pool.AsyncResult`
        """
        reservations = [self.get_reservation(num_instances, script) for script in user_data_scripts]
        paramsets = [(self.conn, reservation) for reservation in reservations]
        async_result = Pool(processes=processes).map_async(get_ids_from_reso_tuple, paramsets)
        return async_result

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


def get_ids_from_reso_tuple(args):
    """
    Here's a simpler approach than the S.O. one that decouples unnecessary state
    It's an example of why FP > OOP for concurrent programming in Python
    """
    return get_instance_ids_from_reservation(*args)
