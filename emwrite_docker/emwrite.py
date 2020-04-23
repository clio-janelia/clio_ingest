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
import io
import traceback
import threading

# allow very large images to be read (up to 1 gigavoxel)
Image.MAX_IMAGE_PIXELS = 1000000000

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
        shard_size  = config_file["shard-size"]

        # read file
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob("raw/" + name)
        pre_image_bin = blob.download_as_string()
        im = Image.open(io.BytesIO(pre_image_bin))
        #pre_image = numpy.asarray(im)
        
        # modify affine to satisfy the pil transform interface
        # (origin should be center -- not the case actually, row1 then row2, and use inverse affine
        # since transform implements a pull transform and not a push transform).

        # modify trans x and trans y
        #affine_trans[4] += (width/2)
        #affine_trans[5] += (height/2)

        # create affine matrix and invert
        affine_mat = np.array([[affine_trans[0], affine_trans[2], affine_trans[4]],
                [affine_trans[1], affine_trans[3], affine_trans[5]],
                [0, 0, 1]])
        mat_inv = np.linalg.inv(affine_mat)
        post_im = im.transform((width, height), Image.AFFINE, data=mat_inv.flatten()[:6], resample=Image.BICUBIC)

        # write aligned png
        blob = bucket.blob("align/" + name)
        with io.BytesIO() as output:
            post_im.save(output, format="PNG")
            blob.upload_from_string(output.getvalue(), content_type="image/png")

        # write temp png tiles
        post_array = np.array(post_im)
        binary_volume = "".encode()
        sizes = []
        for chunky in range(0, height, shard_size):
            for chunkx in range(0, width, shard_size):
                tile = post_array[chunky:(chunky+shard_size), chunkx:(chunkx+shard_size)]
                tile_bytes_io = io.BytesIO()
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

        start_pos = 24 + (len(sizes)+1)*8
        final_binary += start_pos.to_bytes(8, byteorder="little")
        for val in sizes:
            start_pos += val
            final_binary += start_pos.to_bytes(8, byteorder="little")
        final_binary += binary_volume

        # write to cloud
        bucket_temp = storage_client.bucket(bucket_name_temp)
        blob = bucket_temp.blob(str(slicenum))
        blob.upload_from_string(final_binary, content_type="application/octet-stream")

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
        minz  = int(config_file["minz"])
        maxz  = int(config_file["maxz"])
        [width, height]  = json.loads(config_file["bbox"])
        shard_size  = config_file["shard-size"] 
        if shard_size != 1024:
            raise RuntimeError("shard size must be 1024x1024x1024")
        write_raw  = json.loads(config_file["writeRaw"].lower())

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
        maxz  = config_file["maxz"]
        [width, height]  = json.loads(config_file["bbox"])
        shard_size  = config_file["shard-size"] 
        if shard_size != 1024:
            raise RuntimeError("shard size must be 1024x1024x1024")
        write_raw  = json.loads(config_file["writeRaw"].lower())

        # extract 1024x1024x1024 cube based on tile chunk
        zstart = max(shard_size*tile_chunk[2], minz)
        zfinish = min(maxz, zstart+shard_size-1)
    
        storage_client = storage.Client()
        bucket_temp = storage_client.bucket(bucket_tiled_name)
        
        vol3d = None

        # fetch 1024x1024 tile from each imagee 
        def set_image(slice, vol3d=None):
            blob = bucket_temp.blob(str(slice))
            
            # read offset binary
            pre = 24 # start of index
        
            spot = tile_chunk[1]*(ceil(width/shard_size)) + tile_chunk[0]
            start_index = pre + spot * 8
            end_index = start_index + 16 - 1

            im_range = blob.download_as_string(start=start_index, end=end_index)
            start = int.from_bytes(im_range[0:8], byteorder="little")
            end = int.from_bytes(im_range[8:16], byteorder="little", signed=False) - 1
            
            # png blob
            im_data = blob.download_as_string(start=start, end=end)
            im = Image.open(io.BytesIO(im_data))
            img_array = np.array(im)
            height2, width2 = im.height, im.width
           
            #with io.BytesIO() as output:
            #    blob = bucket_temp.blob(str(slice)+".png")
            #    im.save(output, format="PNG")
            #    blob.upload_from_string(output.getvalue(), content_type="image/png")

            if slice == zstart:
                vol3d = np.zeros((zfinish-zstart+1, height2, width2), dtype=np.uint8)
            vol3d[(slice-zstart), :, :] = img_array
            return vol3d

        vol3d = set_image(zstart)

        threads = [threading.Thread(target=set_image, args=(slice, vol3d)) for slice in range(zstart+1, zfinish+1)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()


        # write grayscale for each level
        num_levels = 5
        start = (tile_chunk[0]*shard_size, tile_chunk[1]*shard_size, zstart)

        # put in fortran order
        vol3d = vol3d.transpose((2,1,0))

        def _write_shard(level, start, vol3d, format):
            # get spec for jpeg and post
            dataset = ts.open({
                'driver': 'neuroglancer_precomputed',
                'kvstore': {
                    'driver': 'gcs',
                    'bucket': bucket_name,
                    },
                'path': f"neuroglancer/{format}",
                'context': {
                    'cache_pool': {
                        'total_bytes_limit': 500_000_000
                    }
                },
                'recheck_cached_data': 'open',
                'scale_index': level
            }).result()

            size = vol3d.shape
            dataset = dataset[ts.d['channel'][0]]
            dataset[ start[0]:(start[0]+size[0]), start[1]:(start[1]+size[1]), start[2]:(start[2]+size[2]) ] = vol3d 
        for level in range(num_levels):
            if level == 0:
                # iterate through different 512 cubes since 1024 will not fit in memory
                for iterz in range(0, 1024, 512):
                    for itery in range(0, 1024, 512):
                        for iterx in range(0, 1024, 512):
                            vol3d_temp = vol3d[iterx:(iterx+512), itery:(itery+512), iterz:(iterz+512)]
                            currsize = vol3d_temp.shape
                            if currsize[0] == 0 or currsize[1] == 0 or currsize[2] == 0:
                                continue
                            start_temp = (start[0]+iterx, start[1]+itery, start[2]+iterz) 

                            _write_shard(level, start_temp, vol3d_temp, "jpeg")
                            if write_raw:
                                _write_shard(level, start_temp, vol3d_temp, "raw")
                                        
            else:
                _write_shard(level, start, vol3d, "jpeg")
                if write_raw:
                    _write_shard(level, start, vol3d, "raw")

            # downsample
            start = (start[0]//2, start[1]//2, start[2]//2)
            vol3d = ndimage.interpolation.zoom(vol3d, 0.5)
            currsize = vol3d.shape
            if currsize[0] == 0 or currsize[1] == 0 or currsize[2] == 0:
                break

        r = make_response("success".encode())
        r.headers.set('Content-Type', 'text/html')
        return r
    except Exception as e:
        return Response(traceback.format_exc(), 400)

def create_meta(width, height, minz, maxz, shard_size, isRaw):
    if (width % shard_size) > 0: 
        width += ( 1024 - (width % shard_size))
    if (height % shard_size) > 0: 
        height += ( 1024 - (height % shard_size))
    if ((maxz + 1)  % shard_size) > 0: 
        maxz += ( 1024 - ((maxz+1) % shard_size))

    # !! makes offset 0 since there appears to be a bug in the
    # tensortore driver.

    # load json (don't need tensorflow)
    return {
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
                "minishard_bits" : 0,
                "minishard_index_encoding" : "gzip",
                "preshift_bits" : 9,
                "shard_bits" : 24
             },
             "size" : [ width, height, (maxz+1) ],
             "realsize" : [ width, height, (maxz-minz+1) ],
             "offset" : [0, 0, 0],
             "realoffset" : [0, 0, minz]
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
                "shard_bits" : 21
             },
             "size" : [ width//2, height//2, (maxz+1)//2 ],
             "realsize" : [ width//2, height//2, (maxz-minz+1)//2 ],
             "offset" : [0, 0, 0],
             "realoffset" : [0, 0, minz//2]
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
                "shard_bits" : 21
             },
             "size" : [ width//4, height//4, (maxz+1)//4 ],
             "realsize" : [ width//4, height//4, (maxz-minz+1)//4 ],
             "offset" : [0, 0, 0],
             "realoffset" : [0, 0, minz//4]
          },
          {
             "chunk_sizes" : [
                [ 64, 64, 64 ]
             ],
             "encoding" : "raw" if isRaw else "jpeg",
             "key" : "64.0x64.0x64.0",
             "resolution" : [ 64, 64, 64 ],
             "size" : [ width//8, height//8, (maxz+1)//8 ],
             "realsize" : [ width//8, height//8, (maxz-minz+1)//8 ],
             "offset" : [0, 0, 0],
            "sharding" : {
                "@type" : "neuroglancer_uint64_sharded_v1",
                "hash" : "identity",
                "minishard_bits" : 0,
                "minishard_index_encoding" : "gzip",
                "preshift_bits" : 3,
                "shard_bits" : 21
             },
             "realoffset" : [0, 0, minz//8]
          },
          {
             "chunk_sizes" : [
                [ 64, 64, 64 ]
             ],
             "encoding" : "raw" if isRaw else "jpeg",
             "key" : "128.0x128.0x128.0",
             "resolution" : [ 128, 128, 128 ],
             "size" : [ width//16, height//16, (maxz+1)//16 ],
             "realsize" : [ width//16, height//16, (maxz-minz+1)//16 ],
             "offset" : [0, 0, 0],
             "sharding" : {
                "@type" : "neuroglancer_uint64_sharded_v1",
                "hash" : "identity",
                "minishard_bits" : 0,
                "minishard_index_encoding" : "gzip",
                "preshift_bits" : 0,
                "shard_bits" : 21
             },
             "realoffset" : [0, 0, minz//16]
          }
       ],
       "type" : "image"
    }



if __name__ == "__main__":
    app.run(debug=True,host='0.0.0.0',port=int(os.environ.get('PORT', 8080)))
