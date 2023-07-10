#!/usr/bin/env python3

# Usage: python3 count-barcode-freq.py <fastq_file.gz>
# Example: python3 count-barcode-freq.py sample.fastq.gz
# https://gencore.bio.nyu.edu/how-to-find-out-what-barcodes-are-in-your-undetermined-reads/

from operator import itemgetter
from collections import Counter
from itertools import islice

import sys, gzip

def count_barcodes(path):
    barcodes = {}
    with gzip.open(path) as fastq:
        barcodes = Counter()
        # # Original.
        # for i, line in enumerate(fastq):
        #     # if line.startswith(b'@'):
        #     if i % 4 == 0:
        #         # Sequence identifer (Field 1 of 4)
        #         # print('Checking line for barcode:', line)
        #         bc = line.decode("utf-8").split(':')[-1].strip()
        #         # print(bc, 'from', line)
        #         # if bc not in barcodes:
        #         #     barcodes[bc] = 1
        #         # else:
        #         #     barcodes[bc] += 1
        #         barcodes.update([bc])
        #     else:
        #         # Not sequence identifier.
        #         continue

        # # Use islice instead.
        # for line in islice(fastq, 0, None, 4):
        #     # Sequence identifer (Field 1 of 4)
        #     # print('Checking line for barcode:', line)
        #     bc = line.decode("utf-8").split(':')[-1].strip()
        #     barcodes.update([bc])

        # Check the actual sequence for barcode.
        for line in islice(fastq, 1, None, 4):
            # Sequence identifer (Field 1 of 4)
            # print('Checking line for barcode:', line)
            bc = line.decode("utf-8")[:10]
            barcodes.update([bc])

    # total = sum(barcodes.values())
    # for k, v in sorted(barcodes.items(), key=itemgetter(1)):
    #     print(k, v, round(v/total*100, 2))
    # print(barcodes)
    print(*barcodes.most_common(10), sep='\n')
    print(barcodes.total())

def main():
    # barcodes = {}
    # with gzip.open(sys.argv[1]) as fastq:
    #     for line in fastq:
    #         if not line.startswith(b'@'): continue
    #         bc = line.decode("utf-8").split(':')[-1].strip()
    #         if bc not in barcodes:
    #             barcodes[bc] = 1
    #         else:
    #             barcodes[bc]+=1
    # total = sum(barcodes.values())
    # for k, v in sorted(barcodes.items(), key=itemgetter(1)):
    #     print(k, v, round(v/total*100, 2))

    for i, path in enumerate(sys.argv[1:]):
        print(i, path)
        count_barcodes(path)

    return 0

if __name__ == '__main__':
    sys.exit(main())
