FROM ubuntu:20.10
RUN apt update && apt upgrade --yes
RUN apt install software-properties-common --yes
RUN apt update && apt upgrade --yes
RUN add-apt-repository universe
RUN apt install python2 python2-dev libpq-dev zip --yes
RUN apt install curl --yes
RUN curl https://bootstrap.pypa.io/pip/2.7/get-pip.py --output /tmp/get-pip.py
RUN python2 /tmp/get-pip.py
RUN apt install build-essential --yes
RUN pip2 install virtualenvwrapper
VOLUME /work
WORKDIR /work
ENTRYPOINT ["/bin/bash"]
ENTRYPOINT ["/bin/bash", "do-make-build-in-docker.sh"]
