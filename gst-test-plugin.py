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
from gi.repository import Gst, GObject, GstBase
# Initializes Gstreamer, it's variables, paths

# DEFAULT_ENDPOINT = "xxx"
# DEFAULT_ACCESS = "xxx"
# DEFAULT_SECRET = "xxx"

DEFAULT_ENDPOINT = "http://ceph-route-rook-ceph.apps.jweng-ocp.shiftstack.com"
DEFAULT_ACCESS = "QjdOMFdZNEE3NTc3MUwwMDNZT1M="
DEFAULT_SECRET = "cmlBWFZLa2tIaWhSaTN5Sk5FNGpxaGRlc2ZGWWtwMWZqWFpqR0FrRA=="
DEFAULT_BUCKET = "my-bucket"
DEFAULT_LOCATION = ""

FORMATS = "{RGBx,BGRx,xRGB,xBGR,RGBA,BGRA,ARGB,ABGR,RGB,BGR}"


def ceph_rgw(path, bucket):     
    while True:
        try:
            s3 = boto3.resource('s3',
                        '',
                        use_ssl = False,
                        verify = False,
                        endpoint_url = DEFAULT_ENDPOINT,
                        aws_access_key_id = base64.decodebytes(bytes(DEFAULT_ACCESS,'utf-8')).decode('utf-8'),
                        aws_secret_access_key = base64.decodebytes(bytes(DEFAULT_SECRET, 'utf-8')).decode('utf-8'),
                    )
            
            GB = 1024 ** 3
            
                # Ensure that multipart uploads only happen if the size of a transfer
                # is larger than S3's size limit for nonmultipart uploads, which is 5 GB.
            config = TransferConfig(multipart_threshold=5 * GB, max_concurrency=10, use_threads=True)

            s3.meta.client.upload_file(path, bucket, os.path.basename(path),
                                                    Config=config,
                                                    Callback=ProgressPercentage(path))
            print("S3 Uploading successful")
            break
        except botocore.exceptions.EndpointConnectionError:
            print("Network Error: Please Check your Internet Connection")


class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify we'll assume this is hooked up
        # to a single filename.
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()
#
# Sink element created entirely in python
# Takes file and uploads to ceph rgw 
#
class CephRGW(GstBase.BaseSink):

    GST_PLUGIN_NAME = 'ceph_rgw_sink'

    #plugin description
    __gstmetadata__ = ('Ceph-RGW-Sink','Sink', \
                      'Custom ceph-rgw-sink element', 'Jason Weng')
   

    gst_sink = Gst.PadTemplate.new("sink",
                    Gst.PadDirection.SINK,
                    Gst.PadPresence.ALWAYS,
                    Gst.Caps.from_string(f"video/x-raw,format={FORMATS}"))
    
  

    #to make pad templates visible for plugin, define gsttemplates field
    __gsttemplates__ = (gst_sink)

    __gproperties__ = {
        "endpoint_url": (GObject.TYPE_STRING,
                     "Endpoint url",
                     "A property that contains str",
                     "",  # default
                     GObject.ParamFlags.READWRITE
                     ),
       
        "access_key": (GObject.TYPE_STRING,
                     "Access key",
                     "Access key for ceph rgw",
                     "",  # default
                     GObject.ParamFlags.READWRITE
                     ),
        
        "secret_key": (GObject.TYPE_STRING,
                     "Secret Key",
                     "Secret key for ceph rgw",
                     "",  # default
                     GObject.ParamFlags.READWRITE
                     ),
        
        "bucket": (GObject.TYPE_STRING,
                     "Bucket",
                     "Bucket for ceph rgw",
                     "",  # default
                     GObject.ParamFlags.READWRITE
                     ),

        "location": (GObject.TYPE_STRING,
                     "Location",
                     "Location of file to upload to ceph rgw",
                     "",  # default
                     GObject.ParamFlags.READWRITE
                     ),
    }   
    

    def __init__(self):

        super(CephRGW, self).__init__()

        self.endpoint = DEFAULT_ENDPOINT
        self.access = DEFAULT_ACCESS 
        self.secret = DEFAULT_SECRET 
        self.location = DEFAULT_LOCATION
        self.bucket = DEFAULT_BUCKET

    def do_get_property(self, prop: GObject.GParamSpec):
        if prop.name == 'endpoint_url':
            return self.endpoint
        elif prop.name == 'access_key':
            return self.access
        elif prop.name == 'secret_key':
            return self.secret
        elif prop.name == 'bucket':
            return self.bucket
        elif prop.name == 'location':
            return self.location
        else:
            raise AttributeError('unknown property %s' % prop.name)

    def do_set_property(self, prop: GObject.GParamSpec, value):
        if prop.name == 'endpoint_url':
            self.endpoint = value
        elif prop.name == 'access_key':
            self.access = value
        elif prop.name == 'secret_key':
            self.secret = value
        elif prop.name == 'bucket':
            self.bucket = value 
        elif prop.name == 'location':
            self.location = value
        elif prop.name == 'parts':
            self.parts = value
        else:
            raise AttributeError('unknown property %s' % prop.name)


    def do_render(self, buffer):
        Gst.info("timestamp(buffer):%s" % (Gst.TIME_ARGS(buffer.pts)))
        try:
            ceph_rgw(DEFAULT_LOCATION, DEFAULT_BUCKET)
        except Exception as e:
            logging.error(e)
        return Gst.FlowReturn.OK

GObject.type_register(CephRGW)
__gstelementfactory__ = ("ceph-rgw-sink", CephRGW.GST_PLUGIN_NAME, Gst.Rank.NONE, CephRGW)
