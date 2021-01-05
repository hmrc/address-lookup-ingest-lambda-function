FROM ubuntu:20.10
RUN apt update && apt upgrade --yes
RUN apt install python3 python3-dev libpq-dev virtualenv --yes
RUN apt install build-essential --yes
RUN apt install curl unzip groff --yes
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "/tmp/awscliv2.zip" \
        && cd /tmp \
        && unzip awscliv2.zip \
        && ./aws/install
COPY requirements.txt /tmp/
RUN ln -s /usr/bin/make /usr/bin/make-build
VOLUME /work
WORKDIR /work
ENV AWS_PROFILE="???"
ENV S3_BUCKET="???"
ENTRYPOINT ["/bin/bash"]

