FROM ubuntu:20.10
RUN apt update && apt upgrade --yes
RUN apt install software-properties-common --yes
RUN apt update && apt upgrade --yes
RUN add-apt-repository universe
RUN apt install python2 python2-dev libpq-dev zip --yes
RUN apt install curl --yes
RUN curl https://bootstrap.pypa.io/2.7/get-pip.py --output /tmp/get-pip.py
RUN python2 /tmp/get-pip.py
RUN apt install build-essential --yes
RUN apt install groff --yes
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "/tmp/awscliv2.zip" && cd /tmp && unzip awscliv2.zip && ./aws/install
COPY requirements.txt /tmp/
RUN pip2 install virtualenvwrapper
RUN pip2 install -r /tmp/requirements.txt
RUN ln -s /usr/bin/make /usr/bin/make-build
VOLUME /work
WORKDIR /work
ENV AWS_PROFILE="???"
ENV S3_BUCKET="???"
ENTRYPOINT ["/bin/bash"]
ENTRYPOINT ["/make-build", "build"]
