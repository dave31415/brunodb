from csv import DictWriter


def write_csv(filename, stream):
        fp = open(filename, 'w')
        first = next(stream)
        fieldnames = list(first.keys())
        writer = DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(stream)
        fp.close()
