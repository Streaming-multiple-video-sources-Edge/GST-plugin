import sys
import traceback
import argparse
from argparse import ArgumentParser
import os
import subprocess
import gi
gi.require_version('Gst', '1.0')
from gi.repository import Gst, GObject  # noqa:F401,F402



def run_youtube(input_url):

    youtube_dl_str = "youtube-dl --format " +  "\"best[ext=mp4][protocol=https]\"" + " --get-url " + input_url
    print("YDS: " + youtube_dl_str)
    location = subprocess.check_output(youtube_dl_str, shell=True).decode()
    print("LOCATION:" + location)
    #pipeline_str = "souphttpsrc is-live=true location=\"{0}\" ! filesink location=video_files/video1.mp4 -e".format(location)
    #pipeline_str = "souphttpsrc is-live=true location=\"{0}\" ! cephrgwsink endpoint_url=http://ceph-route-rook-ceph.apps.jweng-ocp.shiftstack.com access_key=QjdOMFdZNEE3NTc3MUwwMDNZT1M= secret_key=cmlBWFZLa2tIaWhSaTN5Sk5FNGpxaGRlc2ZGWWtwMWZqWFpqR0FrRA== bucket=my-bucket location=\"{0}\" parts=6".format(location)
    a = "http://ceph-route-rook-ceph.apps.jweng-ocp.shiftstack.com"
    b ="VUJHR0ROUkhDUUxYREYwNzQxTzg="
    c ="T2tOdVppaGZhTmdOY1BnaXJscjVHVHo5eFhYSGxVa1pIREdVdmhNTg=="
    pipeline_str = "souphttpsrc is-live=true location=\"{0}\" ! cephrgwsink endpointurl={1} accesskey={2} secretkey={3} bucket=jwengbucket".format(location,a,b,c)
    print(pipeline_str)
    return pipeline_str


# Initializes Gstreamer, it's variables, paths
Gst.init(sys.argv)

input_url = input("Enter the URL of the youtube video: ")
command = run_youtube(input_url)

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

                                          
