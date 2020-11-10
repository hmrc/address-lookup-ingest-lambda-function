TARGET_PATH := dist/
ARTIFACT := address-lookup-ingest-lambda-function

build:
	rm -rf $(TARGET_PATH)
	mkdir $(TARGET_PATH)
	zip -r9 dist/${ARTIFACT}.zip . --exclude "*dist/*" --exclude "*.git/*" --exclude "*.idea/*"
	cd $(TARGET_PATH); openssl dgst -sha256 -binary $(ARTIFACT).zip | openssl enc -base64 > $(ARTIFACT).base64sha256

push-s3:
	aws s3 cp $(TARGET_PATH)/$(ARTIFACT).zip s3://$(S3_BUCKET)/$(ARTIFACT).zip --acl=bucket-owner-full-control
	aws s3 cp $(TARGET_PATH)/$(ARTIFACT).base64sha256 s3://$(S3_BUCKET)/$(ARTIFACT).base64sha256 --acl=bucket-owner-full-control --content-type=text/plain
