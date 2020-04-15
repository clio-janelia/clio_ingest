"""Web server that has endpoints to write aligned data to cloud storage.
"""

import os

from flask import Flask, Response, request, make_response, abort
from flask_cors import CORS
import json
import logging
import pwd
from PIL import Image
from google.cloud import storage
import numpy as np
import tensorstore as ts
from math import ceil
from scipy import ndimage

app = Flask(__name__)

# TODO: Limit origin list here: CORS(app, origins=[...])
CORS(app)

logger = logging.getLogger(__name__)

@app.route('/alignedslice', methods=["POST"])
def alignedslice():
    """Read images storeed in bucket/raw/image, apply the affine transformation
    and write result to bucket/align/image and bucket_temp/slice.
    """
    try:
        config_file  = request.get_json()
        
        name = config_file["img"] 
        bucket_name = config_file["dest"] # contains source and destination
        bucket_name_temp = config_file["dest-tmp"] # destination for tiles
        affine_trans = json.loads(config_file["transform"])
        [width, height]  = json.loads(config_file["bbox"])
        slicenum  = config_file["slice"]
        shard_size  = config_file["shard_size"]

        # read file
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob("raw/" + name)
        pre_image_bin = blob.download_to_string()
        im = Image.open(io.BytesIO(pre_image_bin))
        #pre_image = numpy.asarray(im)
        
        # modify affine to satisfy the pil transform interface
        # (origin should be center, row1 then row2, and use inverse affine
        # since transform implements a pull transform and not a push transform).

        # modify trans x and trans y
        affine_trans[4] += (width/2)
        affine_trans[5] += (height/2)

        # create affine matrix and invert
        affine_mat = np.array([[trans[0], trans[2], trans[4]],
                [trans[1], trans[3], trans[5]],
                [0, 0, 1]])
        mat_inv = np.linalg.inv(affine_mat)
        post_im = im.transform((width, height), Image.AFFINE, data=mat_inv.flatten()[:6], resample=Image.BICUBIC)

        # write aligned png
        blob = bucket.blob("align/" + name)
        with io.BytesIO() as output:
            post_im.save(output, format="PNG")
            blob.upload_from_string(output.getvalue())

        # write temp png tiles
        post_array = np.array(post_im)
        binary_volume = "".encode()
        sizes = []
        for chunky in range(0, height, shard_size):
            for chunkx in range(0, width, shard_size):
                tile = post_array[chunky:(chunky+shard_size), chunkx:(chunkx+shard_size)]
                tile_bytes_io = ioBytesIO()
                # save as png
                tile_im = Image.fromarray(tile)
                tile_im.save(tile_bytes_io, format="PNG")
                tile_bytes = tile_bytes_io.getvalue()

                sizes.append(len(tile_bytes))
                binary_volume += tile_bytes

        # pack binary
        final_binary = width.to_bytes(8, byteorder="little")
        final_binary += height.to_bytes(8, byteorder="little")
        final_binary += shard_size.to_bytes(8, byteorder="little")

        start_pos = 24 + len(sizes+1)*8
        final_binary += val.to_bytes(8, byteorder="little")
        for val in sizes:
            start_pos += val
            final_binary += start_pos.to_bytes(8, byteorder="little")
        final_binary += binary_volume

        # write to cloud
        bucket_temp = storage_client.bucket(bucket_name_temp)
        blob = bucket.blob(str(slice))
        blob.upload_from_string(final_binary.decode('utf-8'))

        r = make_response("success".encode())
        r.headers.set('Content-Type', 'text/html')
        return r
    except Exception as e:
        return Response(str(e), 400)

@app.route('/ngmeta', methods=["POST"])
def ngmeta():
    """Write metadata for ng volumes in bucket/neuroglancer/raw and bucket/neuroglancer/jpeg.
    """
    try:
        config_file  = request.get_json()
        
        bucket_name = config_file["dest"] # contains source and destination
        minz  = config_file["minz"]
        maxz  = config_file["maxz"])
        [width, height]  = json.loads(config_file["bbox"])
        shard_size  = config_file["shard_size"] 
        if shard_size != 1024:
            raise RuntimeError("shard size must be 1024x1024x1024")
        write_raw  = config_filee["writeRaw"]

        # write jpeg config to bucket/neuroglancer/raw/info
        storage_client = storage.Client()
        config = create_meta(width, height, minz, maxz, shard_size, False)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob("neuroglancer/jpeg/info")
        blob.upload_from_string(json.dumps(config))
       
        # write raw config to bucket/neuroglancer/raw/info
        if write_raw:
            config = create_meta(width, height, minz, maxz, shard_size, True)
            blob = bucket.blob("neuroglancer/raw/info")
            blob.upload_from_string(json.dumps(config))

        r = make_response("success".encode())
        r.headers.set('Content-Type', 'text/html')
        return r
    except Exception as e:
        return Response(str(e), 400)

@app.route('/ngshard', methods=["POST"])
def ngshard():
    """Write ng pyramid to bucket/neuroglancer/raw and bucket/neuroglancer/jpeg.
    """
    try:
        config_file  = request.get_json()
        
        bucket_name = config_file["dest"] # contains source and destination
        bucket_tiled_name = config_file["source"] # contains image tiles
        tile_chunk = config_file["start"]
        minz  = config_file["minz"]
        maxz  = config_file["maxz"])
        [width, height]  = json.loads(config_file["bbox"])
        shard_size  = config_file["shard_size"] 
        if shard_size != 1024:
            raise RuntimeError("shard size must be 1024x1024x1024")
        write_raw  = config_filee["writeRaw"]

        # extract 1024x1024x1024 cube based on tile chunk
        zstart = max(shard_size*tile_chunk[2], minz)
        zfinish = min(maxz, zstart+shardsize-1)
    
        storage_client = storage.Client()
        bucket_temp = storage_client.buckeet 
        
        vol3d = None

        # fetch 1024x1024 tile from each imagee 
        for slice in range(zstart, zfinish+1):
            blob = bucket_temp.blob(str(slice))
            
            # read offset binary
            start = 24 # start of index
        
            spot = tile_chunk[1]*(ceil(shard_size[0]/1024)) + tile_chunk[0]
            start = start + spot * 8
            end = start + 8

            im_range = blob.download_as_string(start=start, end=end)
            start = int.from_bytes(im_range[0:8], byteorder="little")
            end = int.from_bytes(im_range[8:16], byteorder="little") - 1

            # png blob
            im_data = blob.download_as_string(start=start, end=end)
            im = Image.open(io.BytesIO(im_data))
            img_array = np.array(im)

            if slice == zstart:
                vo3d = np.array((zfinish-zstart+1, height, width))
            vol3d[slice-zstart, :, :] = img_array

        # write grayscale for each level
        num_levels = 5
        start = (tile_chunk[0]*shard_size, tile_chunk[1]*shard_size, zstart)

        for level in range(num_levels):
            # get spec for jpeg and post
            dataset = ts.open({
                'driver': 'neuroglancer_precomputed',
                'kvstore': {
                    'driver': 'gcs',
                    'bucket': bucket_name,
                    },
                'path': 'neuroglancer/jpeg',
                'context': {
                    'cache_pool': {
                        'total_bytes_limit': 100_000_000
                    }
                },
                'recheck_cached_data': 'open',
                'scale_index': level
            }).result()

            size = img_array.shape
            dataset[start[2]:(start[2]+size[0]), start[1]:(start[1]+size[1]), start[0]:(start[0]+size[2])] = img_array


            if write_raw:
                # get spec for raw and post
                dataset = ts.open({
                    'driver': 'neuroglancer_precomputed',
                    'kvstore': {
                        'driver': 'gcs',
                        'bucket': bucket_name,
                        },
                    'path': 'neuroglancer/raw',
                    'context': {
                        'cache_pool': {
                            'total_bytes_limit': 100_000_000
                        }
                    },
                    'recheck_cached_data': 'open',
                    'scale_index': level
                }).result()

                dataset[start[2]:(start[2]+size[0]), start[1]:(start[1]+size[1]), start[0]:(start[0]+size[2])] = img_array

            # downsample
            start = (start[0]//2, start[1]//2, start[2]//2)
            img_array = ndimage.interpolation.zoom(img_array, 0.5)


        r = make_response("success".encode())
        r.headers.set('Content-Type', 'text/html')
        return r
    except Exception as e:
        return Response(str(e), 400)




def create_meta(width, height, minz, maxz, shard_size, isRaw):
    if (width % shard_size) > 0: 
        width += ( 1024 - (width % shard_size))
    if (height % shard_size) > 0: 
        height += ( 1024 - (height % shard_size))
    if (maxz  % shard_size) > 0: 
        maxz += ( 1024 - (maxz % shard_size))

    # load json (don't need tensorflow)
    return config = {
       "@type" : "neuroglancer_multiscale_volume",
       "data_type" : "uint8",
       "num_channels" : 1,
       "scales" : [
          {
             "chunk_sizes" : [
                [ 64, 64, 64 ]
             ],
             "encoding" : "raw" if isRaw else "jpeg",
             "key" : "8.0x8.0x8.0",
             "resolution" : [ 8, 8, 8 ],
             "sharding" : {
                "@type" : "neuroglancer_uint64_sharded_v1",
                "hash" : "identity",
                "minishard_bits" : 3,
                "minishard_index_encoding" : "gzip",
                "preshift_bits" : 9,
                "shard_bits" : 18
             },
             "size" : [ width, height, (maxz-minz+1) ],
             "offset" : [0, 0, minz]
          },
          {
             "chunk_sizes" : [
                [ 64, 64, 64 ]
             ],
             "encoding" : "raw" if isRaw else "jpeg",
             "key" : "16.0x16.0x16.0",
             "resolution" : [ 16, 16, 16 ],
             "sharding" : {
                "@type" : "neuroglancer_uint64_sharded_v1",
                "hash" : "identity",
                "minishard_bits" : 0,
                "minishard_index_encoding" : "gzip",
                "preshift_bits" : 9,
                "shard_bits" : 18
             },
             "size" : [ width//2, height//2, (maxz-minz+1)//2 ],
             "offset" : [0, 0, minz//2]
          },
          {
             "chunk_sizes" : [
                [ 64, 64, 64 ]
             ],
             "encoding" : "raw" if isRaw else "jpeg",
             "key" : "32.0x32.0x32.0",
             "resolution" : [ 32, 32, 32 ],
             "sharding" : {
                "@type" : "neuroglancer_uint64_sharded_v1",
                "hash" : "identity",
                "minishard_bits" : 0,
                "minishard_index_encoding" : "gzip",
                "preshift_bits" : 6,
                "shard_bits" : 18
             },
             "size" : [ width//4, height//4, (maxz-minz+1)//4 ],
             "offset" : [0, 0, minz//4]
          },
          {
             "chunk_sizes" : [
                [ 64, 64, 64 ]
             ],
             "encoding" : "raw" if isRaw else "jpeg",
             "key" : "64.0x64.0x64.0",
             "resolution" : [ 64, 64, 64 ],
             "size" : [ width//8, height//8, (maxz-minz+1)//8 ],
             "offset" : [0, 0, minz//8]
          },
          {
             "chunk_sizes" : [
                [ 64, 64, 64 ]
             ],
             "encoding" : "raw" if isRaw else "jpeg",
             "key" : "128.0x128.0x128.0",
             "resolution" : [ 128, 128, 128 ],
             "size" : [ width//16, height//16, (maxz-minz+1)//16 ],
             "offset" : [0, 0, minz//16]
          }
       ],
       "type" : "image"
    }



if __name__ == "__main__":
    app.run(debug=True,host='0.0.0.0',port=int(os.environ.get('PORT', 8080)))
