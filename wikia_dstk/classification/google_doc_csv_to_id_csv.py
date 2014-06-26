from argparse import ArgumentParser, FileType


def get_args():
    ap = ArgumentParser()
    ap.add_argument('--infile', dest="infile", type=FileType('r'))
    ap.add_argument('--outfile', dest="outfile", type=FileType('w'), metavar="file", default="/tmp/wid_to_class.csv")
    ap.add_argument('--for-secondary', dest="for_secondary", action="store_true", default=False)
    return ap.parse_args()


def primary_transformation(fl):
    results = []
    for line in fl:
        splt = line.split(',')
        results.append(','.join(splt[2:4]))
    return "\n".join(results)


def secondary_transformation(fl):
    results = []
    for line in fl:
        splt = line.split(',')
        [results.append(",".join([splt[2], secondary.strip().lower()])) for secondary in splt[4].split('|')]
    return "\n".join(results)


def transform(fl, for_secondary=False):
    return primary_transformation(fl) if not for_secondary else secondary_transformation(fl)


def main():
    args = get_args()
    args.outfile.write(transform(args.infile, args.for_secondary))
    print args.outfile.abspath


if __name__ == '__main__':
    main()