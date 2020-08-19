# custom gstreamer rgw sink plugin
Streams files from a source and sinks into ceph object storage.
Ceph credentials include: access key, secret key, and endpoint url


# Run containerized plugin and parse code
```
podman run -ti --privileged --net=host -v `pwd`:/work docker.io/jweng1/gst-rgwsink:v1
```


# Run as developer

```
git clone https://github.com/Streaming-multiple-video-sources-Edge/gstreamer-rgw-sink-plugin.git

podman run -ti --privileged --net=host -v `pwd`:/work docker.io/jweng1/gst-base-image:v1

cd work
cd gstreamer-rgw-sink-plugin

python3 -m venv venv
source venv/bin/activate
pip install -U wheel pip setuptools
pip install -r requirements.txt

```

# Export and Inspect plugin
```
export GST_PLUGIN_PATH=$GST_PLUGIN_PATH:$PWD/venv/lib/gstreamer-1.0/:$PWD/gst/
gst-inspect-1.0 python
```
