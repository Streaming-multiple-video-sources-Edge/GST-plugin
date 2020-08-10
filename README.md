# GST-plugin

RUN

```
podman run -ti --privileged --net=host -v `pwd`:/work docker.io/jweng1/gst-test:v1



new parse function:

podman run -ti --privileged --net=host -v `pwd`:/work docker.io/neeha246/gst-image:v1






python3 -m venv venv
source venv/bin/activate
pip install -U wheel pip setuptools

pip install -r requirements.txt

export GST_PLUGIN_PATH=$GST_PLUGIN_PATH:$PWD/venv/lib/gstreamer-1.0/:$PWD/gst/
