"""Usage: python3 delchunk.py BIG_FILE CHUNK_FILE_OR_FOLDER [OUT_FILE]

Given a large file BIG_FILE, delete all complete non-overlapping (and possibly large) chunks given by CHUNK_FILE_OR_FOLDER
While each chunk file is treated statelessly (AABABB-AB=AB), multiple chunk files are treated statefully (ACABDB-AB-CD=AB)
Multiple chunk files will be deleted from the largest to the smallest (and secondarily by chunk filename)
If OUT_FILE is omitted, will do a dryrun
"""


import mmap
import os
import shutil
import sys


if len(sys.argv) < 3:
    print(__doc__)
    sys.exit(1)
dryrun = len(sys.argv) < 4
if dryrun:
    print('(DRYRUN)')
if not dryrun and sys.argv[1] != sys.argv[3]:
    shutil.copy(sys.argv[1], sys.argv[3])
if os.path.isdir(sys.argv[2]):
    chunks = [os.path.join(sys.argv[2], chunk) for chunk in os.listdir(sys.argv[2])]
    chunks = sorted(chunk for chunk in chunks if os.path.isfile(chunk))
    chunks.sort(key=os.path.getsize, reverse=True)
else:
    chunks = [sys.argv[2]]
with open(sys.argv[1 if dryrun else 3], 'rb' if dryrun else 'r+b') as big_file,\
        mmap.mmap(big_file.fileno(), 0, access=mmap.ACCESS_COPY if dryrun else mmap.ACCESS_WRITE) as big_map:
    big_len = len(big_map)
    for chunk in chunks:
        with open(chunk, 'rb') as chunk_file, mmap.mmap(chunk_file.fileno(), 0, access=mmap.ACCESS_READ) as chunk_map:
            chunk_len = len(chunk_map)
            prev_start = big_len
            i = 0
            while big_len >= chunk_len:
                start = big_map.rfind(chunk_map, 0, prev_start)
                if start == -1:
                    break
                i += 1
                end = start + chunk_len
                print('Deleting chunk %s (%d) at %d:%d%s' % (chunk, i, start, end, ' (cont.)' if end == prev_start else ''))
                big_map.move(start, end, big_len - end)
                prev_start = start
                big_len -= chunk_len
                if not dryrun:
                    big_map.resize(big_len)
            if not i:
                print('Chunk %s not found' % chunk)
            elif not dryrun:
                big_map.flush()
if dryrun:
    print('(DRYRUN)')
