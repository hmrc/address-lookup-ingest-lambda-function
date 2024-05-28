TARGET_PATH := target/scala-2.12
ARTIFACT := address-lookup-file-ingest-lambda-functions_2.12-1.0

test:
	sbt test

build:
	sbt clean test assembly
	cd target/scala-2.12; openssl dgst -sha256 -binary $(ARTIFACT).jar | openssl enc -base64 > $(ARTIFACT).base64sha256

push-s3:
	aws s3 cp $(TARGET_PATH)/$(ARTIFACT).jar s3://$(S3_BUCKET)/$(ARTIFACT).jar --acl=bucket-owner-full-control
	aws s3 cp $(TARGET_PATH)/$(ARTIFACT).base64sha256 s3://$(S3_BUCKET)/$(ARTIFACT).base64sha256 --acl=bucket-owner-full-control --content-type=text/plain
	aws s3 cp $(TARGET_PATH)/$(ARTIFACT).base64sha256 s3://$(S3_BUCKET)/$(ARTIFACT).jar.base64sha256 --acl=bucket-owner-full-control --content-type=text/plain
