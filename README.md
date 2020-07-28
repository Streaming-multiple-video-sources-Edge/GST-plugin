# GST-plugin

Set up

```
podman run -ti --privileged -v `pwd`:/work docker.io/rootfs/gst-python 

git clone https://github.com/Streaming-multiple-video-sources-Edge/GST-plugin.git

cd GST-plugin

python3 -m venv venv
source venv/bin/activate

pip install -U wheel pip setuptools
pip install -r requirements.txt

```


Export plugin

```
export GST_PLUGIN_PATH=$GST_PLUGIN_PATH:$PWD/venv/lib/gstreamer-1.0/:$PWD/gst/ 
gst-inspect-1.0 python

```


