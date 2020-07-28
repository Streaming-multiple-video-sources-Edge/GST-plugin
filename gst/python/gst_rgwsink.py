#export GST_PLUGIN_PATH=$GST_PLUGIN_PATH:$PWD

import gi 
import sys
import os 
import argparse
import boto3
import botocore
import base64
import threading
import logging
from boto3.s3.transfer import TransferConfig
gi.require_version('Gst', '1.0')
gi.require_version('GstBase', '1.0')
from gi.repository import Gst, GObject, GstBase


# Initializes Gstreamer, it's variables, paths
GObject.threads_init()
Gst.init(None)

#default variables 
DEFAULT_ENDPOINT = "http://ceph-route-rook-ceph.apps.jweng-ocp.shiftstack.com"
DEFAULT_ACCESS = "QjdOMFdZNEE3NTc3MUwwMDNZT1M="
DEFAULT_SECRET = "cmlBWFZLa2tIaWhSaTN5Sk5FNGpxaGRlc2ZGWWtwMWZqWFpqR0FrRA=="
DEFAULT_BUCKET = "my-bucket"
DEFAULT_PARTS = 6

# DEFAULT_BUCKET = "myBucket"
# DEFAULT_ENDPOINT = "http://ceph-route-rook-ceph.apps.neeha-ocp.shiftstack.com"
# DEFAULT_ACCESS = "SFQ4MzE0SkxKNFRFOUNMTDZPV04="
# DEFAULT_SECRET = "MFV4VUFkb3p3RW5jWXVNZjk2S2lKOXdWTGJMaEdkUVNMQngzb2hMUA=="
# DEFAULT_PARTS = 6

FORMATS = "{RGBx,BGRx,xRGB,xBGR,RGBA,BGRA,ARGB,ABGR,RGB,BGR}"

s3 = None
s3r = None
mpu = None   # Multipart upload handle

#
# Thread (safe) function responsible of uploading a part of the file
#

def upload_part_r(partid, part_start, part_end, thr_args):
    filename = thr_args['FileName']
    bucket = thr_args['BucketName']
    upload_id = thr_args['UploadId']

    logging.info("%d: >> Uploading part %d", partid, partid)
    logging.info("%d: --> Upload starts at byte %d", partid, part_start)
    logging.info("%d: --> Upload ends at byte %d", partid, part_end)

    f = open(filename, "rb")
    logging.info("%d: DEBUG: Seeking offset: %d", partid, part_start)
    logging.info("%d: DEBUG: Reading size: %d", partid, part_end - part_start)
    f.seek(part_start, 0)
    
    data = f.read(part_end - part_start + 1)
    part = s3.upload_part(Bucket=bucket, Key=filename, PartNumber=partid+1, UploadId=upload_id, Body=data)

    # Thread critical variable which should hold all information about ETag for all parts, access to this variable
    # should be synchronized.
    lock = thr_args['Lock']
    if lock.acquire():
            thr_args['PartInfo']['Parts'].append({'PartNumber': partid+1, 'ETag': part['ETag']})
            lock.release()

    f.close()
    logging.info("%d: -><- Part ID %d is ending", partid, partid)
    return

      
#
# Part size calculations.
# Thread dispatcher
#
def handle_mp_file(bucket, filename, nrparts):

    print(">> Uploading file: " + filename + ", nr_parts = " + str(nrparts))

    fsize = os.path.getsize(filename)
    print("+ %s file size = %d " % (filename, fsize))

    npart = nrparts
    # do the part size calculations
    while(1):
        part_size = int(fsize / npart)
        print("+ standard part size = " + str(part_size) + " bytes")
        if (part_size > 5242880):
            print("The new bucket size is " + str(npart) + "because the parts have to be minimum 5 MB")
            nrparts = npart
            break
        npart = npart - 1


    mpu = s3.create_multipart_upload(Bucket=bucket, Key=filename)

    threads = list()
    thr_lock = threading.Lock()
    thr_args = { 'PartInfo': { 'Parts': [] } , 'UploadId': mpu['UploadId'], 'BucketName': bucket, 'FileName': filename,
            'Lock': thr_lock }

    for i in range(nrparts):
        print("++ Part ID: " + str(i))

        part_start = i * part_size
        part_end = (part_start + part_size) - 1

        if (i+1) == nrparts:
            print("DEBUG: last chunk, part-end was/will %d/%d" % (part_end, fsize))
            part_end = fsize

        print("DEBUG: part_start=%d/part_end=%d" % (part_start, part_end))

        thr = threading.Thread(target=upload_part_r, args=(i, part_start, part_end, thr_args, ) )
        threads.append(thr)
        thr.start()

    # Wait for all threads to complete
    for index, thr in enumerate(threads):
        thr.join()
        print("%d thread finished" % (index))

    part_info = thr_args['PartInfo']
    
    for p in part_info['Parts']:
        print("DEBUG: PartNumber=%d" % (p['PartNumber']))
        print("DEBUG: ETag=%s" % (p['ETag']))

    print("+ Finishing up multi-part uploads")
    s3.complete_multipart_upload(Bucket=bucket, Key=filename, UploadId=mpu['UploadId'], MultipartUpload=thr_args['PartInfo'])
    return True 
           
#sink element 
class CephRGW(GstBase.BaseSink):

    #plugin name
    GST_PLUGIN_NAME = 'cephrgwsink'
    __gtype_name__ = 'cephrgwsink'
    
    #plugin description
    __gstmetadata__ = ('cephrgwsinkSink', #name
                      'Sink element', #transform
                      'A sink element to store data to ceph rgw storage', #description
                      'Jason Weng, Neeha Kompala') #authors 
   
    #to make pad templates visible for plugin, define gsttemplates field
    __gsttemplates__  = Gst.PadTemplate.new("sink",
                        Gst.PadDirection.SINK,
                        Gst.PadPresence.ALWAYS,
                        Gst.Caps.from_string(f"video/x-raw,format={FORMATS}"))
    
    #object properties 
    __gproperties__ = {
        "endpointurl": (GObject.TYPE_STRING,
                     "Endpoint url",
                     "A property that contains str",
                     DEFAULT_ENDPOINT,  # default
                     GObject.ParamFlags.READWRITE
                     ),
       
        "accesskey": (GObject.TYPE_STRING,
                     "Access key",
                     "Access key for ceph rgw",
                     DEFAULT_ACCESS,  # default
                     GObject.ParamFlags.READWRITE
                     ),
        
        "secretkey": (GObject.TYPE_STRING,
                     "Secret Key",
                     "Secret key for ceph rgw",
                     DEFAULT_SECRET,  # default
                     GObject.ParamFlags.READWRITE
                     ),
        
        "bucket": (GObject.TYPE_STRING,
                     "Bucket",
                     "Bucket for ceph rgw",
                     DEFAULT_BUCKET,  # default
                     GObject.ParamFlags.READWRITE
                     ),

        "parts": (GObject.TYPE_INT64,
                     "Parts",
                     "How many threads to use",
                     0, #min
                     100, #max 
                     DEFAULT_PARTS,  # default
                     GObject.ParamFlags.READWRITE
                     )

    }


    def __init__(self, *args):
        GstBase.BaseSink.__init__(self, *args)

        self.endpoint_url = DEFAULT_ENDPOINT
        self.access_key = DEFAULT_ACCESS 
        self.secret_key = DEFAULT_SECRET 
        self.bucket = DEFAULT_BUCKET
        self.parts = DEFAULT_PARTS

    def do_get_property(self, prop: GObject.GParamSpec):
        if prop.name == 'endpointurl':
            return self.endpoint_url
        elif prop.name == 'accesskey':
            return self.access_key
        elif prop.name == 'secretkey':
            return self.secret_key
        elif prop.name == 'bucket':
            return self.bucket
        elif prop.name == 'parts':
            return self.parts
        else:
            raise AttributeError('unknown property %s' % prop.name)

    def do_set_property(self, prop: GObject.GParamSpec, value):
        if prop.name == 'endpointurl':
            self.endpoint_url = value
        elif prop.name == 'accesskey':
            self.access_key = value
        elif prop.name == 'secretkey':
            self.secret_key = value
        elif prop.name == 'bucket':
            self.bucket = value 
        elif prop.name == 'parts':
            self.parts = value
        else:
            raise AttributeError('unknown property %s' % prop.name)


    def do_render(self, buffer):
        Gst.info("timestamp(buffer):%s" % (Gst.TIME_ARGS(buffer.pts)))
        		
        try:
            # Initialize the connection with Ceph RADOS GW
            s3 = boto3.client(service_name = 's3', use_ssl = False, verify = False,
                            endpoint_url = self.endpoint_url,
                            aws_access_key_id = base64.decodebytes(bytes(self.access_key,'utf-8')).decode('utf-8'),
                            aws_secret_access_key = base64.decodebytes(bytes(self.secret_key,'utf-8')).decode('utf-8'),)
            s3r = boto3.resource(service_name = 's3', use_ssl = False, verify = False, endpoint_url = self.endpoint_url,
                            aws_access_key_id = base64.decodebytes(bytes(self.access_key,'utf-8')).decode('utf-8'),
                            aws_secret_access_key = base64.decodebytes(bytes(self.secret_key,'utf-8')).decode('utf-8'),)

            response = s3.list_buckets()
            # Get a list of all bucket names from the response
            buckets = [bucket['Name'] for bucket in response['Buckets']]

            # Print out the bucket list
            print("Initial bucket List: %s" % buckets)

            #s3r.Bucket("MyBucket").objects.all().delete()
            #s3.delete_bucket(Bucket="MyBucket")

            print("Trying to make 'mybucket'")
            if self.bucket not in buckets:
                s3.create_bucket(Bucket=self.bucket)
            else:
                print("Bucket " + self.bucket + " already exists, deleting and recreating")
                s3r.Bucket(self.bucket).objects.all().delete()
                s3.delete_bucket(Bucket=self.bucket)
                s3.create_bucket(Bucket=self.bucket)

            response = s3.list_buckets()
            #Get all buckets 
            buckets = [bucket['Name'] for bucket in response['Buckets']]

            # Print out the bucket list
            print("Updated bucket List: %s" % buckets)


            handle_mp_file(self.bucket, buffer, self.parts)
            
          
        except Exception as e:
            logging.error(e)
        return Gst.FlowReturn.OK


# Required for registering plugin dynamically
GObject.type_register(CephRGW)
__gstelementfactory__ = (CephRGW.GST_PLUGIN_NAME, Gst.Rank.NONE, CephRGW)
