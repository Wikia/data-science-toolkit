import os
from boto.ec2 import connect_to_region
from time import sleep

def chrono_sort(directory):
    """Return a list of files in a directory, sorted chronologically"""
    files = [(os.path.join(directory, filename), os.path.getmtime(os.path.join(directory, filename))) for filename in os.listdir(directory)]
    files.sort(key=lambda x: x[1])
    return files

class EC2Connection(object):
    """A connection to a specified EC2 region."""
    def __init__(self, options):
        """
        Instantiate a boto.ec2.connection.EC2Connection object upon which to
        perform actions.

        :type options: dict
        :param region: A dictionary containing the autoscaling options
        """
        self.region = options.get('region')
        self.price = options.get('price')
        self.ami = options.get('ami')
        self.key = options.get('key')
        self.sec = options.get('sec')
        if not isinstance(self.sec, list):
            self.sec = self.sec.split(',')
        self.type = options.get('type')
        self.tag = options.get('tag')
        self.threshold = options.get('threshold')
        self.max_size = options.get('max_size')
        self.conn = connect_to_region(self.region)

    def _request_instances(self, count):
        """
        Request spot instances.

        :type count: int
        :param count: The number of spot instances to request

        :rtype: boto.ec2.instance.Reservation
        :return: The Reservation object representing the spot instance request
        """
        return self.conn.request_spot_instances(price=self.price,
                                                image_id=self.ami,
                                                count=count,
                                                key_name=self.key,
                                                security_groups=self.sec,
                                                instance_type=self.type)

    def _get_instance_ids(self, reservation):
        """
        Get instance IDs for a particular reservation.

        :type reservation: boto.ec2.instance.Reservation
        :param reservation: A Reservation object created by requesting spot
                            instances

        :rtype: list
        :return: A list containing strings representing the instance IDs of the
                 given Reservation
        """
        r_ids = [request.id for request in reservation]
        while True:
            sleep(5)
            requests = self.conn.get_all_spot_instance_requests(request_ids=r_ids)
            instance_ids = []
            for request in requests:
                instance_id = request.instance_id
                if instance_id is None:
                    break
                instance_ids.append(instance_id)
            if len(instance_ids) < len(reservation):
                print 'waiting for %d instances to launch...' % len(reservation)
                continue
            break
        return instance_ids

    def _tag_instances(self, instance_ids):
        """
        Attach identifying tags to the specified instances.

        :type instance_ids: list
        :param instance_ids: A list of instance IDs to tag
        """
        tags = {'Name': self.tag}
        self.conn.create_tags(instance_ids, tags)

    def add_instances(self, count):
        """
        Add a specified number of instances.

        :type count: int
        :param count: The number of instances to add

        :rtype: int
        :return: An integer indicating the number of active tagged instances
        """
        # Create spot instances
        reservation = self._request_instances(count)
        # Tag created spot instances
        instance_ids = self._get_instance_ids(reservation)
        self._tag_instances(instance_ids)

        return len(self.get_tagged_instances())

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
