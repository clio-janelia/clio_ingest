"""Ingestsa a small stack of pre-aligned/registered 2D RGBA images into NG precomputed.

This script is a proof a concept and designed for smaller volumes (e.g. < 10GB)
and could be incorporated into the no-align mode of the Clio ingestion workflow for those
interesting in scaling it up.  For CLEM applicaions, the light data will generally be much
smaller than the corresponding EM dataset so a simple standalone script
might suffice for ingestion.

usage:

python ingest_multichannel_small.py image_prefix minz maxz bucket path resolution 

Notes:

1. A custom shader needs to be set in neuroglancer to view RGB.  This shader is embedded in the written info file for convenience.
2. The number of scale levels is hard-coded.


"""

import sys
import tensorstore as ts
from PIL import Image
import numpy as np
from google.cloud import storage
import json
from skimage.transform import downscale_local_mean

# read args
image_prefix = sys.argv[1]
minz = int(sys.argv[2])
maxz = int(sys.argv[3])
bucket = sys.argv[4]
path = sys.argv[5]
res = int(sys.argv[6])

vol = None

# hard code number of levels for now (also the config file is hard coded to two levels)
NUM_LEVELS = 2

# read volume
for iter1 in range(minz, maxz+1):
    im = Image.open(image_prefix % iter1)
    arr = np.array(im)
    im.mode = "RGBA"
    arr = np.array(im)
    if vol is None:
        y,x,_ = arr.shape
        vol = np.zeros((maxz-minz+1, y, x, 4), dtype=np.uint8) 
    vol[iter1, :, :, :] = arr

z, y, x, ch = vol.shape

# put in xyzch order
vol = vol.transpose((2,1,0,3))
vol = vol[:,:,:,0:3]

# construct config
config = f'{{"@type": "neuroglancer_multiscale_volume", "data_type": "uint8", "num_channels": 3, "shader": "void main () {{ emitRGB(vec3(toNormalized(getDataValue(0)), toNormalized(getDataValue(1)), toNormalized(getDataValue(2))));}}", "scales": [{{"chunk_sizes": [[64, 64, 64]], "encoding": "jpeg", "key": "{res}.0x{res}.0x{res}.0", "resolution": [{res}, {res}, {res}], "sharding": {{"@type": "neuroglancer_uint64_sharded_v1", "hash": "identity", "minishard_bits": 0, "minishard_index_encoding": "gzip", "preshift_bits": 6, "shard_bits": 27}}, "size": [{x}, {y}, {z}], "offset": [0, 0, 0], "realoffset": [0, 0, 0]}}, {{"chunk_sizes": [[64, 64, 64]], "encoding": "jpeg", "key": "{res*2}.0x{res*2}.0x{res*2}.0", "resolution": [{res*2}, {res*2}, {res*2}], "sharding": {{"@type": "neuroglancer_uint64_sharded_v1", "hash": "identity", "minishard_bits": 0, "minishard_index_encoding": "gzip", "preshift_bits": 6, "shard_bits": 24}}, "size": [{x//2}, {y//2}, {z//2}], "offset": [0, 0, 0], "realoffset": [0, 0, 0]}}], "type": "image"}}'

# post config
client = storage.Client()
bucket = client.bucket(bucket)
blob = bucket.blob(f"{path}/info")
blob.upload_from_string(json.dumps(json.loads(config)))

# post data for each level
for level in range(NUM_LEVELS):
    dataset = ts.open({"driver": "neuroglancer_precomputed", "kvstore": {"driver": "gcs", "bucket": "clio_clem_test"}, "path": path, "recheck_cached_data": "open", "scale_index": level}).result()
    x, y, z, _ = vol.shape
    dataset[0:x, 0:y, 0:z, 0:3] = vol
    # downsample
    vol = downscale_local_mean(vol, (2,2,2,1)).astype(vol.dtype, copy=False)[:x//2, :y//2, :z//2]

