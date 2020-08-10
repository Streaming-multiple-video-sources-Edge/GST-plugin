import sys
import traceback
import argparse
from argparse import ArgumentParser
import os
import subprocess
import gi
gi.require_version('Gst', '1.0')
from gi.repository import Gst, GObject  # noqa:F401,F402


def run_youtube(endpoint, accesskey, secretkey, bucket, partsize, key, input_url,limit):
    youtube_dl_str = "youtube-dl --format " +  "\"best[ext=mp4][protocol=https]\"" + " --get-url " + input_url
    print("YDS: " + youtube_dl_str)
    loc = subprocess.check_output(youtube_dl_str, shell=True).decode()
    print("LOCATION: " + loc)
    print("++++++++++++++++++++++++++++++++++++++++++++++++++++")
    #e,a,s,b,p,k
    e = endpoint
    a = accesskey
    s = secretkey
    b = bucket
    p = partsize
    k = key
    o = limit

    pipeline_str = "souphttpsrc is-live=true location={0} ! cephrgwsink endpointurl={1} accesskey={2} secretkey={3} bucket={4} partsize={5} key={6} limitsize={7}".format(loc,e,a,s,b,p,k,o)
    print(pipeline_str)
    return pipeline_str


def on_message(bus: Gst.Bus, message: Gst.Message, loop: GObject.MainLoop):
    mtype = message.type
    """
        Gstreamer Message Types and how to parse
        https://lazka.github.io/pgi-docs/Gst-1.0/flags.html#Gst.MessageType
    """
    if mtype == Gst.MessageType.EOS:
        print("End of stream")
        loop.quit()

    elif mtype == Gst.MessageType.ERROR:
        err, debug = message.parse_error()
        print(err, debug)
        loop.quit()

    elif mtype == Gst.MessageType.WARNING:
        err, debug = message.parse_warning()
        print(err, debug)

    return True

def call_pipeline(command):
    # Initializes Gstreamer, it's variables, paths
    Gst.init(sys.argv)
    #count = 0

    pipeline = Gst.parse_launch(command)

    bus = pipeline.get_bus()

    # allow bus to emit messages to main thread
    bus.add_signal_watch()

    # Start pipeline
    pipeline.set_state(Gst.State.PLAYING)

    # Init GObject loop to handle Gstreamer Bus Events
    loop = GObject.MainLoop()

    # Add handler to specific signal
    bus.connect("message", on_message, loop)

    try:
        loop.run()
    except Exception:
        traceback.print_exc()
        loop.quit()

    # Stop Pipeline
    pipeline.set_state(Gst.State.NULL)


# Initializes Gstreamer, it's variables, paths
#Gst.init(sys.argv)
count = 0

#endpoint, access, secret, bucket, partsize, key
endpoint = input("Enter your endpoint url (with http): ")
accesskey = input("Enter your accesskey: ")
secretkey = input("Enter your secretkey: ")
#bucket = input("Enter your bucket name or type 'default' for default (mybucket): ")
partsize = input("Enter the partsize or type 'default' for default (5mb = 5*1024*1024): ")
#key = input("Enter a nickname for the file you want to upload: ")
limit = input("Enter the maxium upload limit: ")

while(True):

    input_url = input("Enter the URL of the youtube video or type 'done' to finish: ")
    if(input_url == 'done'):
        break
    bucket = input("Enter your bucket name or type 'default' for default (mybucket): ")
    key = input("Enter a nickname for the file you want to upload: ")

    count += 0
    command = run_youtube(endpoint, accesskey, secretkey, bucket, partsize, key, input_url,limit)
    call_pipeline(command)
