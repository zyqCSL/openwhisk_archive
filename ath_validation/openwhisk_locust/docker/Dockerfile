FROM locustio/locust
RUN  apt-get update \
     # Upgrade installed packages to get latest security fixes if the base image does not contain them already.
     && apt-get upgrade -y --no-install-recommends \
     # cleanup package lists, they are not used anymore in this image
     && rm -rf /var/lib/apt/lists/* \
     # install additional python modules
     && pip3 install --upgrade pip3 setuptools six && pip3 install --no-cache-dir -r requirements.txt