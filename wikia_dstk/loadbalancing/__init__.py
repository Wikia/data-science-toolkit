from boto.ec2 import connect_to_region
from boto.s3.connection import S3Connection
from boto.s3.key import Key
from collections import defaultdict
from multiprocessing import Pool
from time import sleep
from uuid import uuid4

INSTANCE_LIMIT = 20


def get_instance_ids_from_reservation(conn, reservation):
    """
    Get instance IDs from a reservation

    :type conn: class:`boto.ec2.ec2connection`
    :param conn: an EC2 connection

    :type reservation: class:`boto.ec2.spotinstancerequest.SpotInstanceRequest`
    :param reservation: a spot instance request reservation

    :rtype: list
    :return: a list of EC2 instance IDs
    """

    # Get instance IDs for the reservation
    r_ids = [request.id for request in reservation]
    while True:  # Because the requests are fulfilled independently
        sleep(15)
        requests = conn.get_all_spot_instance_requests(request_ids=r_ids)
        instance_ids = [request.instance_id for request in requests if
                        request.instance_id]
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
                      goes. Should contain '{key}' to pass an S3 filepath as an
                      argument via a string-formatting operation

    :type options: dict
    :param options: Launch configuration options with which to instantiate an
                    EC2Connection object

    :type ami: string
    :param ami: The AMI ID of the image to load

    :rtype: multiprocessing.pool.AsyncResult
    :return: multiprocessing.pool.AsyncResult
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
        formatted = user_data.format(key=k.key)
        scripts.append(formatted)

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
        return self.conn.request_spot_instances(
            price=self.price, image_id=self.ami, count=count,
            key_name=self.key, security_groups=self.sec, user_data=user_data,
            instance_type=self.type)

    def add_instances(self, count, user_data=None, instance_type="dstk_general"):
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
        instance_ids = get_instance_ids_from_reservation(self.conn, reservation)
        self.tag_instances(instance_ids, instance_type=instance_type)
        return instance_ids

    def tag_instances(self, instance_ids, instance_type="dstk_general"):
        """
        Tag instances with tag provided
        :type instance_ids: list
        :param instance_ids: a list of instance ids
        """
        self.conn.create_tags(instance_ids, {'Name': self.tag, "type": instance_type})

    def add_instances_async(self, user_data_scripts, num_instances=1,
                            processes=2, wait=True):
        """
        Add a specified number of instances asynchronously, each with unique
        user_data.

        :type user_data_scripts: iterable
        :param user_data_scripts: strings representing the scripts to
                                  run on the individual instances

        :type num_instances: int
        :param num_instances: The number of instances to spawn PER USER DATA
                              SCRIPT

        :type processes: int
        :param processes: The number of processes to use

        :type wait: bool
        :param wait: whether to wait to complete in the event of an error

        :rtype:
        :return:`multiprocessing.pool.AsyncResult`
        """
        scripts = map(lambda x: x, user_data_scripts)
        while True:
            # Find pending, running, shutting-down, stopping instances
            active_instances = filter(
                lambda x: x.state_code in (0, 16, 32, 64),
                self.conn.get_only_instances())
            desired_instances = len(active_instances) + len(scripts)
            # Find spot instance requests with associated instances active
            active_sirs = filter(
                lambda x: x.status.code != 'instance-terminated-by-user',
                self.conn.get_all_spot_instance_requests())
            desired_sirs = len(active_sirs) + len(scripts)
            if (desired_instances < INSTANCE_LIMIT and
                    desired_sirs < INSTANCE_LIMIT):
                print 'Intended totals:, %d instances, %d spot requests' % (
                    desired_instances, desired_sirs)
                break
            if wait:
                print 'Up: %d instances, %d spot requests. Sleeping 30 sec' % (
                    len(active_instances), len(active_sirs))
                sleep(30)
                continue
            print 'Limit exceeded: %d instances, %d spot requests' % (
                len(active_instances), len(active_sirs))
            raise Exception('Too many active instances or spot requests')

        reservations = []
        for script in scripts:
            reservations.append(self.get_reservation(num_instances, script))

        paramsets = [(self.conn, reservation) for reservation in reservations]
        async_result = Pool(processes=processes).map_async(
            get_ids_from_reso_tuple, paramsets)
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

    def ensure_instance_health(self, tag=None):
        """
        Ensure that all tagged instances are passing status checks, and reboot
        any that aren't.

        :type tag: string
        :param tag: A string representing the tag name to operate over
        """
        tagged = self.get_tagged_instances(tag)
        statuses = self.conn.get_all_instance_status(tagged)
        impaired = filter(lambda x: (x.system_status.status == 'impaired' or
                                     x.instance_status.status == 'ok'),
                          statuses)
        if impaired:
            self.conn.reboot_instances([i.id for i in impaired])


def get_ids_from_reso_tuple(args):
    """
    Here's a simpler approach than the S.O. one that decouples unnecessary state
    It's an example of why FP > OOP for concurrent programming in Python
    """
    return get_instance_ids_from_reservation(*args)

