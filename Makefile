TARGET_PATH := dist/
ARTIFACT := address-lookup-ingest-lambda-function
ROOT_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

build:
	rm -rf $(TARGET_PATH)
	mkdir $(TARGET_PATH)
	zip dist/${ARTIFACT}.zip constants.py lambda_function.py split_abp_files.py
	zip dist/${ARTIFACT}.zip *.sql
	zip -r dist/${ARTIFACT}.zip psycopg2
	zip -r dist/${ARTIFACT}.zip Crypto
	cd venv/lib/python2.7/site-packages && zip ${ROOT_DIR}/dist/${ARTIFACT}.zip credstash.py #&& zip -r ${ROOT_DIR}/dist/${ARTIFACT}.zip Crypto
	cd $(TARGET_PATH); openssl dgst -sha256 -binary $(ARTIFACT).zip | openssl enc -base64 > $(ARTIFACT).base64sha256

push-s3:
	aws s3 cp $(TARGET_PATH)/$(ARTIFACT).zip s3://$(S3_BUCKET)/$(ARTIFACT).zip --acl=bucket-owner-full-control
	aws s3 cp $(TARGET_PATH)/$(ARTIFACT).base64sha256 s3://$(S3_BUCKET)/$(ARTIFACT).base64sha256 --acl=bucket-owner-full-control --content-type=text/plain
