from argparse import ArgumentParser, FileType


def get_argparser():
    ap = ArgumentParser()
    ap.add_argument('--infile', dest='infile', type=FileType('r'), help="A newline-separated file of wiki IDs")
    ap.add_argument('--s3path', dest='s3path', help="The path of an existing list of wiki IDs on s3")
    ap.add_argument('--num-authority-nodes', dest='num_authority_nodes', type=int, default=8,
                    help="Number of authority nodes to spin off")
    ap.add_argument('--num-data-extraction-nodes', dest='num_data_extraction_nodes', type=int, default=2,
                    help="Number of data extraction nodes to spin off")
    ap.add_argument('--authority-ami', dest='ami', default="ami-5488e864", help='AMI for node')
    ap.add_argument('--dstk-ami', dest='ami', default="ami-000f6d30", help='AMI for node')
    ap.add_argument('--dstk-git-ref', dest='dstk_git_ref', default='master',
                    help="Git ref to have checked out for dstk")
    ap.add_argument('--authority-git-ref', dest='authority_git_ref', default='master',
                    help="Git ref to have checked out for Wikia Authority")
    ap.add_argument('--overwrite', dest='overwrite', default=False, action='store_true',
                    help="Whether to overwrite existing cached responses")
    return ap