#!/usr/bin/env sh
virtualenv -p python2 venv
source venv/bin/activate
pip install -r requirements.txt
make build


