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
from skimage import exposure
import gc
import gzip
from skimage.transform import downscale_local_mean

import time
import psutil
from datetime import datetime
def profile(tag):
    mem = str(psutil.virtual_memory())
    return f"{tag}: {mem} {datetime.now()}\n"

# allow very large images to be read (up to 1 gigavoxel)
Image.MAX_IMAGE_PIXELS = 1000000000

app = Flask(__name__)

# TODO: Limit origin list here: CORS(app, origins=[...])
CORS(app)
logger = logging.getLogger(__name__)

MAX_IMAGE_SIZE = 4096
MAX_SUPERIMAGE_SIZE = 12288
OVERLAP_SIZE = 512

# modify clahe to return re-scaled 16 bit image
#exposure._adapthist.img_as_float = lambda x: x

@app.route('/alignedslice', methods=["POST"])
def alignedslice():
    """Read images storeed in bucket/image, apply the affine transformation
    and write result to bucket/align/image and bucket_temp/slice.
    """
    try:
        config_file  = request.get_json()

        def clahe(im, pad_x0, pad_x1, pad_y0, pad_y1, glb_min, glb_max, is_xstart=True, is_ystart=True, is_xend=True, is_yend=True):
            im = np.array(im)
            gc.collect()

            # tile size
            CLAHE_SIZE = 3072
            h, w = im.shape
            target = np.zeros_like(im)

            for y in range(pad_y0, h-pad_y1, CLAHE_SIZE):
                for x in range(pad_x0, w-pad_x1, CLAHE_SIZE):
                    ystart = max(0, y-OVERLAP_SIZE)
                    xstart = max(0, x-OVERLAP_SIZE)

                    # spread out 0 first over range
                    im_sub = im[ystart:(y+CLAHE_SIZE+OVERLAP_SIZE), xstart:(x+CLAHE_SIZE+OVERLAP_SIZE)]
            
                    # if all 0 skip
                    if len(im_sub[im_sub != 0]) == 0:
                        continue

                    # make sure range is the same for all images
                    if ystart  > 0 or xstart > 0:
                        im_sub[0, 0] = glb_min
                        im_sub[1, 0] = glb_max
                    elif ((y + CLAHE_SIZE + OVERLAP_SIZE) < h) or ((x + CLAHE_SIZE + OVERLAP_SIZE) < w):
                        im_sub[-1, -1] = glb_min
                        im_sub[-1, -2] = glb_max

                    """Erase 0s from iamge
                    min_val = im_sub[im_sub != 0].min()
                    max_val = im_sub.max()
                    
                    # create a random matrix between min and max to spread out values
                    im_sub = np.random.randint(min_val, max_val + 1, im_sub.shape, np.uint8)
                    im_sub[ im[ystart:(y+CLAHE_SIZE+OVERLAP_SIZE), xstart:(x+CLAHE_SIZE+OVERLAP_SIZE)] != 0 ] = 0
                    im_sub = im_sub + im[ystart:(y+CLAHE_SIZE+OVERLAP_SIZE), xstart:(x+CLAHE_SIZE+OVERLAP_SIZE)] 
                    """

                    # use modified image to run clahe
                    im_sub = (exposure.equalize_adapthist(im_sub, kernel_size = 1024, clip_limit=0.02)*255).astype(np.uint8)
                    
                    # reset zeros
                    im_sub[ im[ystart:(y+CLAHE_SIZE+OVERLAP_SIZE), xstart:(x+CLAHE_SIZE+OVERLAP_SIZE)] == 0 ] = 0

                    #ys, xs = im_sub.shape
                    #target[ystart:(ystart+ys), xstart:(xstart+xs)] = im_sub

                    t_ystart = y
                    t_xstart = x
                    t_yend = y+CLAHE_SIZE
                    t_xend = x+CLAHE_SIZE
                    if ystart == 0 and is_ystart:
                        t_ystart = 0
                    if xstart == 0 and is_xstart:
                        t_xstart = 0

                    if t_yend >= (h - OVERLAP_SIZE) and is_yend:
                        t_yend = h
                    if t_xend >= (w - OVERLAP_SIZE) and is_xend:
                        t_xend = w

                    target[t_ystart:t_yend, t_xstart:t_xend] = im_sub[(t_ystart-ystart):((t_ystart-ystart)+(t_yend-t_ystart)), (t_xstart-xstart):((t_xstart-xstart)+(t_xend-t_xstart))]


            im = Image.fromarray(target)
            del target
            gc.collect()
            return im

        name = config_file["img"] 
        bucket_name = config_file["dest"] # contains source
        run_id = config_file["run_id"] # contains id for job run for caching thumbnails

        bucket_name_temp = config_file["dest-tmp"] # destination for tiles
        affine_trans = json.loads(config_file["transform"])
        [width, height]  = json.loads(config_file["bbox"])
        slicenum  = config_file["slice"]
        shard_size  = config_file["shard-size"]

        # read file
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(name)
        pre_image_bin = blob.download_as_string()
        curr_im = Image.open(io.BytesIO(pre_image_bin))
        del pre_image_bin


        GLB_MIN, GLB_MAX =  curr_im.getextrema()
        GLB_MIN = 0 # always assume pressence of 0 pixel somewhere

        # make small thumbnail for first tile or only tile
        # (mostly for debugging or quick viewing in something like fiji)
        bucket_thumb = storage_client.bucket(bucket_name + "_process")
        blob = bucket_thumb.blob(run_id + "/align/" + name)
        TARGET_SIZE = 4096
        with io.BytesIO() as output:
            max_dim = max(width, height)
            factor = 1
            while max_dim > TARGET_SIZE:
                max_dim = max_dim // 2
                factor *= 2
            im_small = curr_im
            if factor > 1:
                im_small = curr_im.resize((width//factor, height//factor), resample=Image.BICUBIC)
           
            affine_mat = np.array([[affine_trans[0], affine_trans[2], affine_trans[4]//factor],
            [affine_trans[1], affine_trans[3], affine_trans[5]//factor],
            [0, 0, 1]])
            mat_inv = np.linalg.inv(affine_mat)
            im_small = im_small.transform((width//factor, height//factor), Image.AFFINE, data=mat_inv.flatten()[:6], resample=Image.BICUBIC, fillcolor=0)
            
        
            # normalize image (even though potentially downsampled heavily)
            
            im_small = clahe(im_small, 0, 0, 0, 0, GLB_MIN, GLB_MAX) 
            #im_small = Image.fromarray((exposure.equalize_adapthist(np.array(im_small), kernel_size=1024)*255).astype(np.uint8))
        
            # write output to bucket
            im_small.save(output, format="PNG")
            blob.upload_from_string(output.getvalue(), content_type="image/png")
        
        ####### Iterate per super tile chunk #######

        #super_tile_chunk = config_file["super-tile-chunk"]
        super_tile_chunks = []
        
        for itery in range(0, height, MAX_SUPERIMAGE_SIZE):
            for iterx in range(0, width, MAX_SUPERIMAGE_SIZE):
                super_tile_chunks.append([iterx//MAX_SUPERIMAGE_SIZE, itery//MAX_SUPERIMAGE_SIZE])
        orig_width, orig_height = width, height
        master_im = curr_im
        orig_affine_trans = affine_trans.copy()

        for super_tile_chunk in super_tile_chunks:
            # modify width and heigh if tiled
            startx = starty = trail_x = trail_y = 0
            curr_im = master_im
            width, height = orig_width, orig_height
            affine_trans = orig_affine_trans.copy()

            # new width and height
            if width > (MAX_SUPERIMAGE_SIZE):
                width = width - super_tile_chunk[0]*MAX_SUPERIMAGE_SIZE

                # add buffer if needed
                if width > MAX_SUPERIMAGE_SIZE:
                    width = MAX_SUPERIMAGE_SIZE + OVERLAP_SIZE
                    trail_x = OVERLAP_SIZE
                
                width += OVERLAP_SIZE
                startx = OVERLAP_SIZE
                affine_trans[4] -= (super_tile_chunk[0]*MAX_SUPERIMAGE_SIZE - OVERLAP_SIZE)

                if width % shard_size != 0:
                    leftover = (shard_size - (width % shard_size))
                    width += leftover
                    trail_x += leftover

            if height > (MAX_SUPERIMAGE_SIZE):
                height = height - super_tile_chunk[1]*MAX_SUPERIMAGE_SIZE

                # add buffer if needed
                if height > MAX_SUPERIMAGE_SIZE:
                    height = MAX_SUPERIMAGE_SIZE + OVERLAP_SIZE 
                    trail_y = OVERLAP_SIZE
                
                height += OVERLAP_SIZE
                starty = OVERLAP_SIZE
                affine_trans[5] -= (super_tile_chunk[1]*MAX_SUPERIMAGE_SIZE - OVERLAP_SIZE)
                
                if height % shard_size != 0:
                    leftover = (shard_size - (height % shard_size))
                    height += leftover
                    trail_y += leftover

            # modify affine to satisfy the pil transform interface
            # (origin should be center -- not the case actually, row1 then row2, and use inverse affine
            # since transform implements a pull transform and not a push transform).
            # create affine matrix and invert
            affine_mat = np.array([[affine_trans[0], affine_trans[2], affine_trans[4]],
                    [affine_trans[1], affine_trans[3], affine_trans[5]],
                    [0, 0, 1]])
            mat_inv = np.linalg.inv(affine_mat)
            curr_im = curr_im.transform((width, height), Image.AFFINE, data=mat_inv.flatten()[:6], resample=Image.BICUBIC, fillcolor=0)
          
            is_startx = is_starty = is_endx = is_endy = False
            if super_tile_chunk[0] == 0:
                is_startx = True
            if super_tile_chunk[1] == 0:
                is_starty = True
            if super_tile_chunk[0] == super_tile_chunks[-1][0]:
                is_endx = True
            if super_tile_chunk[1] == super_tile_chunks[-1][1]:
                is_endy = True


            curr_im = clahe(curr_im, startx, trail_x, starty, trail_y, GLB_MIN, GLB_MAX, is_startx, is_starty, is_endx, is_endy)
            #curr_im = Image.fromarray((exposure.equalize_adapthist(np.array(curr_im), kernel_size=1024)//255).astype(np.uint8))

            # ?? is result better with single thread, single clahe, single write
            NUM_THREADS = 4
            # write sub-image into tile chunks (group together to reduce IO)
            # TODO: add overlap betwen tiles for CLAHE calculation
            failure = None
            def write_sub_image_tiles(thread_id):
                nonlocal failure
                try:
                    # write temp png tiles
                    job_id = -1
                    
                    for y in range(starty, (height - trail_y), MAX_IMAGE_SIZE):
                        for x in range(startx, (width - trail_x), MAX_IMAGE_SIZE):
                            # determine which thread gets the job
                            job_id += 1
                            if (job_id % NUM_THREADS) != thread_id:
                                continue

                            binary_volume = "".encode()
                            sizes = []
                            for chunky in range(y, min(y+MAX_IMAGE_SIZE, height), shard_size):
                                for chunkx in range(x, min(x+MAX_IMAGE_SIZE, width), shard_size):
                                    tile = np.array(curr_im.crop((chunkx-OVERLAP_SIZE, chunky-OVERLAP_SIZE, chunkx+shard_size+OVERLAP_SIZE, chunky+shard_size+OVERLAP_SIZE)))
                                    #tile = (exposure.equalize_adapthist(tile, kernel_size=1024)*255).astype(np.uint8)
                                    tile = tile[OVERLAP_SIZE:-OVERLAP_SIZE, OVERLAP_SIZE:-OVERLAP_SIZE]
                                    tile_bytes_io = io.BytesIO()
                                    # save as png
                                    tile_im = Image.fromarray(tile)
                                    tile_im.save(tile_bytes_io, format="PNG")
                                    tile_bytes = tile_bytes_io.getvalue()

                                    sizes.append(len(tile_bytes))
                                    binary_volume += tile_bytes

                            # pack binary
                            final_binary = orig_width.to_bytes(8, byteorder="little")
                            final_binary += orig_height.to_bytes(8, byteorder="little")
                            final_binary += shard_size.to_bytes(8, byteorder="little")

                            start_pos = 24 + (len(sizes)+1)*8
                            final_binary += start_pos.to_bytes(8, byteorder="little")
                            for val in sizes:
                                start_pos += val
                                final_binary += start_pos.to_bytes(8, byteorder="little")
                            final_binary += binary_volume

                            # file offset
                            xoffset = x // MAX_IMAGE_SIZE
                            yoffset = y // MAX_IMAGE_SIZE
                            xoffset = (super_tile_chunk[0] * MAX_SUPERIMAGE_SIZE) // MAX_IMAGE_SIZE + xoffset 
                            yoffset = (super_tile_chunk[1] * MAX_SUPERIMAGE_SIZE) // MAX_IMAGE_SIZE + yoffset 

                            # write to cloud
                            bucket_temp = storage_client.bucket(bucket_name_temp)
                            blob = bucket_temp.blob(f"{slicenum}_{xoffset}_{yoffset}")
                            blob.upload_from_string(final_binary, content_type="application/octet-stream")
                except Exception as e:
                    failure = e
                    raise
            # write superblocks to disk in chunks of MAX_IMAGE_SIZE
            threads = [threading.Thread(target=write_sub_image_tiles, args=(thread_id,)) for thread_id in range(NUM_THREADS)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            #write_sub_image_tiles(0)
            if failure is not None:
                raise failure

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
        bucket_name_raw = config_file["dest_raw"] # contains source and destination
        minz  = int(config_file["minz"])
        maxz  = int(config_file["maxz"])
        res = int(config_file["resolution"])
        [width, height]  = json.loads(config_file["bbox"])
        shard_size  = config_file["shard-size"] 
        if shard_size != 1024:
            raise RuntimeError("shard size must be 1024x1024x1024")
        write_raw  = json.loads(config_file["writeRaw"].lower())

        # write jpeg config to bucket/neuroglancer/jpeg/info
        storage_client = storage.Client()
        config = create_meta(width, height, minz, maxz, shard_size, False, res)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob("neuroglancer/jpeg/info")
        blob.upload_from_string(json.dumps(config))
       
        # write raw config to bucket/neuroglancer/raw/info
        if write_raw:
            config = create_meta(width, height, minz, maxz, shard_size, True, res)
            bucket = storage_client.bucket(bucket_name_raw)
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
        bucket_name_raw = config_file["dest_raw"] # contains source and destination
        bucket_tiled_name = config_file["source"] # contains image tiles
        tile_chunk = config_file["start"]
        resolution = config_file["resolution"]
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

        #storage_client = storage.Client()
        #bucket_temp = storage_client.bucket(bucket_tiled_name)
        
        vol3d = None
        failure = None

        assert((MAX_IMAGE_SIZE % shard_size) == 0)
        def set_image(slice, zstart, zfinish):
            try:
                nonlocal vol3d
                nonlocal failure
                
                storage_client = storage.Client()
                bucket_temp = storage_client.bucket(bucket_tiled_name)
                
                # x and y block location
                x_block = (tile_chunk[0]*shard_size) // MAX_IMAGE_SIZE
                y_block = (tile_chunk[1]*shard_size) // MAX_IMAGE_SIZE

                # setup offsets for finding shards
                chunk_tile_chunk_0 = ((tile_chunk[0]*shard_size) %  MAX_IMAGE_SIZE) // shard_size
                chunk_tile_chunk_1 = ((tile_chunk[1]*shard_size) %  MAX_IMAGE_SIZE) // shard_size
                chunk_width = MAX_IMAGE_SIZE // shard_size
                if (width - x_block*MAX_IMAGE_SIZE) < MAX_IMAGE_SIZE:
                    chunk_width = ceil((width - x_block*MAX_IMAGE_SIZE) / shard_size)

                # get image block
                blob = bucket_temp.blob(str(slice))
                blob = bucket_temp.blob(f"{slice}_{x_block}_{y_block}")
                
                # read offset binary
                pre = 24 # start of index
            
                spot = chunk_tile_chunk_1*chunk_width + chunk_tile_chunk_0
                start_index = pre + spot * 8
                end_index = start_index + 16 - 1

                im_range = blob.download_as_string(start=start_index, end=end_index)
                start = int.from_bytes(im_range[0:8], byteorder="little")
                end = int.from_bytes(im_range[8:16], byteorder="little", signed=False) - 1
                
                # png blob
                tries = 5
                found = False
                while not found and tries > 0:
                    tries -= 1
                    try:
                        im_data = blob.download_as_string(start=start, end=end)
                        found = True
                    except Exception:
                        time.sleep(2)
                        pass

                if not found:
                    raise Exception("File not found")

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

            except Exception as e:
                failure = e
                raise

        # number of downsample levels
        num_levels = 6

        # fetch 1024x1024 tile from each imagee
        def set_images(start, finish, zstart_vol, thread_id, num_threads):
            for slice in range(start, finish+1):
                if (slice % num_threads) == thread_id:
                    set_image(slice, zstart_vol, finish)

        def _write_shard(level, start, vol3d, format, dataset=None):
            """Method to write shard through tensorstore.
            """

            if dataset is None:
                # get spec for jpeg and post
                dataset = ts.open({
                    'driver': 'neuroglancer_precomputed',
                    'kvstore': {
                        'driver': 'gcs',
                        'bucket': bucket_name,
                        },
                    'path': f"neuroglancer/{format}",
                    'recheck_cached_data': 'open',
                    'scale_index': level
                }).result()
                dataset = dataset[ts.d['channel'][0]]

            size = vol3d.shape
            dataset[ start[0]:(start[0]+size[0]), start[1]:(start[1]+size[1]), start[2]:(start[2]+size[2]) ] = vol3d 
            return dataset 
        

        storage_client = storage.Client()
        bucket_raw = storage_client.bucket(bucket_name_raw)
        def _write_shard_raw(vol3d, offset):
            """Write gzip 512x512x512 in ng format.
            """

            start = [tile_chunk[0]*shard_size + offset[0]*512, tile_chunk[1]*shard_size + offset[1]*512, glb_zstart + offset[2]*512]

            blob = bucket_raw.blob(f"neuroglancer/raw/{resolution}.0x{resolution}.0x{resolution}.0/{start[0]}-{start[0]+512}_{start[1]}-{start[1]+512}_{start[2]}-{start[2]+512}")
            blob.content_encoding = "gzip"
            tarr = np.zeros((512, 512, 512), dtype=np.uint8)
            tarr[0:vol3d.shape[0], 0:vol3d.shape[1], 0:vol3d.shape[2]] = vol3d
            blob.upload_from_string(gzip.compress(tarr.tostring()), content_type="application/octet-stream")

        def _downsample(vol, mode="constant"):
            """Downsample piecewise.
            """
            x,y,z = vol.shape

            # just call interpolate over whole volume if large enough
            if x <= 256 and y <= 256 and z <= 256:
                #return ndimage.interpolation.zoom(vol, 0.5, order=1, mode=mode)
                return downscale_local_mean(vol, (2,2,2)).astype(vol.dtype, copy=False)
            
            target = np.zeros((round(x/2),round(y/2),round(z/2)), dtype=np.uint8)
            for xiter in range(0, x, 256):
                for yiter in range(0, y, 256):
                    for ziter in range(0, z, 256):
                        #target[(xiter//2):((xiter+256)//2), (yiter//2):((yiter+256)//2), (ziter//2):((ziter+256)//2)] = ndimage.interpolation.zoom(vol[xiter:(xiter+256),yiter:(yiter+256),ziter:(ziter+256)], 0.5, order=1, mode=mode)
                        target[(xiter//2):((xiter+256)//2), (yiter//2):((yiter+256)//2), (ziter//2):((ziter+256)//2)] = downscale_local_mean(vol[xiter:(xiter+256),yiter:(yiter+256),ziter:(ziter+256)], (2,2,2)).astype(vol.dtype, copy= False)

            return target 
        
        ####### Iterate 512 slices at a time ########
        glb_zstart = zstart
        glb_zfinish = zfinish

        for zstart in range(glb_zstart, glb_zfinish+1, 512):
            zfinish = zstart + 512 - 1
            if zfinish > glb_zfinish:
                zfinish = glb_zfinish

            # set first image
            set_image(zstart, zstart, zfinish)

            # use 20 threads in parallel to fetch
            num_threads = 20
            threads = [threading.Thread(target=set_images, args=(zstart+1, zfinish, zstart, thread_id, num_threads)) for thread_id in range(num_threads)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

            if failure is not None:
                raise Exception("tile not fetched")

            # write grayscale for each level
            start = (tile_chunk[0]*shard_size, tile_chunk[1]*shard_size, zstart)

            #storage_client2 = storage.Client()
            #bucket = storage_client2.bucket(bucket_name)

            gc.collect()
            if write_raw:
                if glb_zstart % shard_size == 0:
                    # only support shard aligned now
                    for iterz in range((zstart-glb_zstart), (zstart-glb_zstart) + 512, 512):
                        for itery in range(0, 1024, 512):
                            for iterx in range(0, 1024, 512):
                                vol3d_temp = vol3d[(iterz%512):((iterz%512)+512), itery:(itery+512), iterx:(iterx+512)]
    
                                # ignore if empty
                                currsize = vol3d_temp.shape
                                if currsize[0] == 0 or currsize[1] == 0 or currsize[2] == 0:
                                    continue
                                _write_shard_raw(vol3d_temp, (iterx//512, itery//512, iterz//512))
            gc.collect()

            # put in fortran order
            vol3d = vol3d.transpose((2,1,0))

            for level in range(num_levels):
                if level == 0:
                    # iterate through different 256 cubes since 1024 will not fit in memory
                    dataset_jpeg = None
                    for iterz in range((zstart-glb_zstart), (zstart-glb_zstart) + 512, 256):
                        for itery in range(0, 1024, 256):
                            for iterx in range(0, 1024, 256):
                                vol3d_temp = vol3d[iterx:(iterx+256), itery:(itery+256), (iterz%512):((iterz%512)+256)]
                                currsize = vol3d_temp.shape
                                if currsize[0] == 0 or currsize[1] == 0 or currsize[2] == 0:
                                    continue
                                start_temp = (start[0]+iterx, start[1]+itery, start[2]+(iterz%512)) 
                                
                                _ = _write_shard(level, start_temp, vol3d_temp, "jpeg", dataset_jpeg)
                else:
                    _ = _write_shard(level, start, vol3d, "jpeg")

                # downsample
                #vol3d = ndimage.interpolation.zoom(vol3d, 0.5)
                mode = "constant"
                if level >= 4:
                    mode = "nearest" 
                vol3d = _downsample(vol3d, mode)
                start = (start[0]//2, start[1]//2, start[2]//2)
                currsize = vol3d.shape
                if currsize[0] == 0 or currsize[1] == 0 or currsize[2] == 0:
                    break

        r = make_response("success".encode())
        r.headers.set('Content-Type', 'text/html')
        return r
    except Exception as e:
        return Response(traceback.format_exc(), 400)

def create_meta(width, height, minz, maxz, shard_size, isRaw, res):
    if (width % shard_size) > 0: 
        width += ( 1024 - (width % shard_size))
    if (height % shard_size) > 0: 
        height += ( 1024 - (height % shard_size))
    if ((maxz + 1)  % shard_size) > 0: 
        maxz += ( 1024 - ((maxz+1) % shard_size))

    # !! makes offset 0 since there appears to be a bug in the
    # tensortore driver.

    # !! make jpeg chunks 256 cubes (return to 512, maybe,
    # when tensorstore issues are addressed)

    # !! refactor to use unsharded format for raw (just save
    # 256 chunks) and for 64 and 128 cubes for jpeg (currently
    # a bug with unsharded pieces in ng)

    if isRaw:
        return {
                "@type" : "neuroglancer_multiscale_volume",
                "data_type" : "uint8",
                "num_channels" : 1,
                "scales" : [
                    {
                        "chunk_sizes" : [
                            [ 512, 512, 512 ]
                            ],
                        "encoding" : "raw",
                        "key" : f"{res}.0x{res}.0x{res}.0",
                        "resolution" : [ res, res, res ],
                        "size" : [ width, height, (maxz+1) ],
                        "realsize" : [ width, height, (maxz-minz+1) ],
                        "offset" : [0, 0, 0],
                        "realoffset" : [0, 0, minz]
                    }
                ],
                "type" : "image"
            }

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
             "key" : f"{res}.0x{res}.0x{res}.0",
             "resolution" : [ res, res, res ],
             "sharding" : {
                "@type" : "neuroglancer_uint64_sharded_v1",
                "hash" : "identity",
                "minishard_bits" : 0,
                "minishard_index_encoding" : "gzip",
                "preshift_bits" : 6,
                "shard_bits" : 27
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
             "key" : f"{res*2}.0x{res*2}.0x{res*2}.0",
             "resolution" : [ res*2, res*2, res*2 ],
             "sharding" : {
                "@type" : "neuroglancer_uint64_sharded_v1",
                "hash" : "identity",
                "minishard_bits" : 0,
                "minishard_index_encoding" : "gzip",
                "preshift_bits" : 6,
                "shard_bits" : 24
             },
             "size" : [ width//2+1, height//2+1, (maxz+1)//2+1 ],
             "realsize" : [ width//2, height//2, (maxz-minz+1)//2 ],
             "offset" : [0, 0, 0],
             "realoffset" : [0, 0, minz//2]
          },
          {
             "chunk_sizes" : [
                [ 64, 64, 64 ]
             ],
             "encoding" : "raw" if isRaw else "jpeg",
             "key" : f"{res*4}.0x{res*4}.0x{res*4}.0",
             "resolution" : [ res*4, res*4, res*4 ],
             "sharding" : {
                "@type" : "neuroglancer_uint64_sharded_v1",
                "hash" : "identity",
                "minishard_bits" : 0,
                "minishard_index_encoding" : "gzip",
                "preshift_bits" : 6,
                "shard_bits" : 21
             },
             "size" : [ width//4+2, height//4+2, (maxz+1)//4+2 ],
             "realsize" : [ width//4, height//4, (maxz-minz+1)//4 ],
             "offset" : [0, 0, 0],
             "realoffset" : [0, 0, minz//4]
          },
          {
             "chunk_sizes" : [
                [ 64, 64, 64 ]
             ],
             "encoding" : "raw" if isRaw else "jpeg",
             "key" : f"{res*8}.0x{res*8}.0x{res*8}.0",
             "resolution" : [ res*8, res*8, res*8 ],
             "size" : [ width//8+4, height//8+4, (maxz+1)//8+4 ],
             "realsize" : [ width//8, height//8, (maxz-minz+1)//8 ],
             "offset" : [0, 0, 0],
             "realoffset" : [0, 0, minz//8]
          },
          {
             "chunk_sizes" : [
                [ 64, 64, 64 ]
             ],
             "encoding" : "raw" if isRaw else "jpeg",
             "key" : f"{res*16}.0x{res*16}.0x{res*16}.0",
             "resolution" : [ res*16, res*16, res*16 ],
             "size" : [ width//16+8, height//16+8, (maxz+1)//16+8 ],
             "realsize" : [ width//16, height//16, (maxz-minz+1)//16 ],
             "offset" : [0, 0, 0],
             "realoffset" : [0, 0, minz//16]
          },
          {
             "chunk_sizes" : [
                [ 64, 64, 64 ]
             ],
             "encoding" : "raw" if isRaw else "jpeg",
             "key" : f"{res*32}.0x{res*32}.0x{res*32}.0",
             "resolution" : [ res*32, res*32, res*32 ],
             "size" : [ width//32+16, height//32+16, (maxz+1)//32+16 ],
             "realsize" : [ width//32, height//32, (maxz-minz+1)//32 ],
             "offset" : [0, 0, 0],
             "realoffset" : [0, 0, minz//16]
          }
       ],
       "type" : "image"
    }



if __name__ == "__main__":
    app.run(debug=True,host='0.0.0.0',port=int(os.environ.get('PORT', 8080)))
