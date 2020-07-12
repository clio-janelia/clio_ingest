"""Create ordered symlink from inLens directory.

Usage: 

% python create_symlinks.py <image dir> <new dir>


To copy a set of images (for example the first 1000) to gbucket:

% cd <new dir>
% gsutil -m cp img.00[0-9][0-9][0-9].png gs://<bucket>/raw/

note: GOOGLE_APPLICATION_CREDENTIALS should be set and gsutil config
run first with the correct project id.  It makes sense to use a very
powerful machine with a lot of memory and network bandwidth, which should
allow 1GB/s to be transferred.  Parallel writes on different machines
on different image ranges is another option.
"""

import sys
import glob
import os

src = sys.argv[1]
dest = sys.argv[2]

# get images from file source
file_list = glob.glob(src + "/*")
file_list.sort()

print(f"Number of images: {len(file_list)}")
os.system(f"mkdir -p {dest}")

# rename and symlink
for num, filepath in enumerate(file_list):
    val = "%05d" % num
    os.system(f"ln -s {filepath} {dest}/img.{val}.png")
