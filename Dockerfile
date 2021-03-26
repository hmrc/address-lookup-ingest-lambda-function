FROM python:2.7.16
RUN apt-get update && apt-get install --yes build-essential bash zip
RUN pip2 install virtualenvwrapper
VOLUME /work
WORKDIR /work
ENTRYPOINT ["/bin/bash", "do-make-build-in-docker.sh"]
