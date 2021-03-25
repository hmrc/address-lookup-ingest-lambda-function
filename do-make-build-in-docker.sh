#!/usr/bin/env sh
[ -d venv ] && rm -r venv
virtualenv -p python2 venv

source venv/bin/activate

pip install -q -r requirements.txt

make build


