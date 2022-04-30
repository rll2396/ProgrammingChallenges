import csv
import sys

import pandas as pd

CHUNK_SIZE = 1e6

def main():
    fpaths = sys.argv[1:]
    
    # Write header for only first chunk
    write_header = True

    for fpath in fpaths:
        fname = fpath.split("/")[-1]
        # There is no interaction between rows, so we can simply process and
        # output the data in chunks.
        for chunk in pd.read_csv(fpath, chunksize=CHUNK_SIZE):
            chunk["filename"] = fname
            chunk.to_csv(sys.stdout, header=write_header, index=False,
                    doublequote=False, escapechar="\\", quoting=csv.QUOTE_ALL,
                    line_terminator="\n")
            write_header = False

if __name__ == "__main__":
    main()
