import logging
import subprocess
import sys
import random
import traceback
from argparse import ArgumentParser, FileType
from boto import connect_s3
from boto.ec2 import connect_to_region
from boto.utils import get_instance_metadata

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())
fh = logging.FileHandler('etl_scaled.log')
fh.setLevel(logging.ERROR)
log.addHandler(fh)


class Unbuffered:

    def __init__(self, stream):
        self.stream = stream

    def write(self, data):
        self.stream.write(data)
        self.stream.flush()

    def __getattr__(self, attr):
        return getattr(self.stream, attr)


def get_args():
    ap = ArgumentParser()
    ap.add_argument('--infile', dest='infile', type=FileType('r'))
    ap.add_argument('--s3file', dest='s3file')
    ap.add_argument('--overwrite', dest='overwrite', action='store_true', default=False)
    ap.add_argument('--die-on-complete', dest='die_on_complete', action='store_true', default=False)
    ap.add_argument('--emit-events', dest='emit_events', action='store_true', default=False)
    ap.add_argument('--event-size', dest='event_size', type=int, default=10)
    return ap.parse_args()


def main():
    sys.stdout = Unbuffered(sys.stdout)
    bucket = connect_s3().get_bucket('nlp-data')
    failed_events = open('/var/log/authority_failed.txt', 'a')

    args = get_args()
    if args.s3file:
        fname = args.s3file.split('/')[-1]
        bucket.get_key(args.s3file).get_file(open(fname, 'w'))
        fl = open(fname, 'r')
    else:
        fl = args.infile

    events = []
    for line in fl:
        wid = line.strip()
        key = bucket.get_key(key_name='service_responses/%s/WikiAuthorityService.get' % wid)
        if (not args.overwrite) and (key is not None and key.exists()):
            log.info("Key exists for %s" % wid)
            continue
        log.info("Wiki %s" % wid)
        try:
            log.info(subprocess.call("python api_to_database.py --wiki-id=%s --processes=64" % wid, shell=True))
            events.append(wid)
        except Exception as e:
            log.error(u"%s %s" % (e, traceback.format_exc()))
            failed_events.write(line)

        if args.emit_events and len(events) >= args.event_size:
            keyname = 'authority_extraction_events/%d' % random.randint(0, 100000000)
            bucket.new_key(keyname).set_contents_from_string("\n".join(events))
            events = []

    if args.emit_events and len(events) > 0:
        keyname = 'authority_extraction_events/%d' % random.randint(0, 100000000)
        bucket.new_key(keyname).set_contents_from_string("\n".join(events))

    if args.s3file:
        bucket.delete_key(args.s3file)

    if args.die_on_complete:
        current_id = get_instance_metadata()['instance-id']
        ec2_conn = connect_to_region('us-west-2')
        ec2_conn.terminate_instances([current_id])


if __name__ == '__main__':
    main()
