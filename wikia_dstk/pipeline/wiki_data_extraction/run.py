from boto import connect_s3
from boto.ec2 import connect_to_region
from boto.utils import get_instance_metadata
from subprocess import Popen
from time import sleep
from wikia_dstk import get_argparser_from_config, argstring_from_namespace
from config import config


def get_args():
    ap = get_argparser_from_config(config)
    ap.add_argument('-s', '--s3path', dest='s3path',
                    help="The location of the wikis file on S3")
    ap.add_argument('-q', '--queue', dest='event_queue',
                    help="The an event queue to poll for files")
    return ap.parse_known_args()


def iterate_wids_from_args(args):
    bucket = connect_s3().get_bucket('nlp-data')
    while True:
        if args.s3path:
            k = bucket.get_key(args.s3path)
            if k is None:
                raise StopIteration
            wids = [wid.strip() for wid in
                    k.get_contents_as_string().split(',')]
            k.delete()
            yield wids
        elif args.event_queue:
            tmp_folder = (args.event_queue.strip('/').split('/')[0] +
                          '/processing/')
            for key in bucket.list(prefix=args.event_queue.strip('/')+'/'):
                try:
                    new_key = tmp_folder+key.name
                    key.copy('nlp-data', new_key)
                    key.delete()
                    new_key_contents = [
                        wid.strip() for wid in
                        bucket.get_key(new_key).get_contents_as_string().split(
                            "\n") if wid]
                    # probably want to do this after completion, but whatever
                    bucket.delete_key(new_key)
                    yield new_key_contents
                except:
                    continue
            raise StopIteration
        else:
            raise Exception("Please specify either s3path or queue")


def main():
    #sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
    args, extras = get_args()
    processes = []
    shutdown_counter = 0
    while True:
        for wids in iterate_wids_from_args(args):
            shutdown_counter = 0
            print "Working on %d wids" % len(wids)
            while len(wids) > 0:
                while len(processes) < 8:
                    if wids:
                        wid = wids.pop()
                        print 'Launching child to process %s' % wid
                        cmdstring = (
                            ('/usr/bin/python -m ' +
                             'wikia_dstk.pipeline.wiki_data_extraction.child' +
                             ' --wiki-id=%s %s') % (
                                str(wid), argstring_from_namespace(args,
                                                                   extras)))
                        processes.append(Popen(cmdstring, shell=True))
                    else:
                        print 'No more wiki IDs to iterate over'
                        break

                processes = filter(lambda x: x.poll() is None, processes)
                sleep(5)

        if len(processes) > 0:
            print len(processes), "processes still running"
            processes = filter(lambda x: x.poll() is None, processes)
            sleep(30)
        else:
            shutdown_counter += 1
            if shutdown_counter == 10:
                print ("Waited five minutes with nothing in the queue, " +
                       "shutting down")
                current_id = get_instance_metadata()['instance-id']
                ec2_conn = connect_to_region(args.region)
                ec2_conn.terminate_instances([current_id])
            sleep(30)


if __name__ == '__main__':
    main()
