
# address-lookup-ingest-lambda

This lambda pre-processes the downloaded files and loads them into the database.

# Building
In order to build, you will need a virtualenv environment - follow:
```shell script
virtualenv --python=<python_version> venv
source venv/bin/activate
pip install -r requirements.txt
```
where `<python_version>` is currently `2.7`.

If building on `macOS` (or anything other than `Linux`) then you will need to build a docker 
image from (Dockerfile)[docker/p2/py2.Dockerfile]. Once the image is built, it can be run as below:

```shell
AWS_PROFILE=<aws profile to use> S3_BuCKET=<s3 bucket that goes with the profile> docker run -it 
--rm --volume $(pwd):/work 
--volume ${HOME}/.aws:/root/.aws -e AWS_PROFILE -e S3_BUCKET --entrypoint /bin/bash p27
```

Once inside the container:
```shell
virtualenv -p python2 venv
source venv/bin/activate
pip install -r requirements.txt
make build
make push-s3
```
This will setup the correct environment and build and push the lambda archive. You will be asked 
for your `aws` `mfa` code.

# Running
The various lambda functions need the following entries in `credstash` for a given environment:

|Secret Name|Description|Example Value|
|-----------|-----------|-------------|
|address_lookup_rds_host|The `rds endpoint` for the instance|`addresslookup.abcd123.eu-west-2.rds.amazonaws.com`|
|address_lookup_rds_database|The name of the database used by this lambda|`addressbasepremium`|
|address_lookup_rds_ingest_user|The application user that will ingest the data|`addresslookupingester`|
|address_lookup_rds_admin_user|The `rds` `master user` used for initial setup of application user|`root`|
|address_lookup_rds_admin_password|The `rds` `master password` used for initial setuup of application user/`???`/


### License

This code is open source software licensed under the [Apache 2.0 License]("http://www.apache.org/licenses/LICENSE-2.0.html").
