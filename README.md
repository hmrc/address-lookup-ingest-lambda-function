dummy-pr
# address-lookup-ingest-lambda

This lambda pre-processes the downloaded files and loads them into the database.

# Building
```shell script
make build
```
Once build:
```shell
make push-s3
```

# Running
The various lambda functions need the following entries in `credstash` for a given environment:

|Secret Name|Description|Example Value|
|-----------|-----------|-------------|
|address_lookup_rds_host|The `rds endpoint` for the instance|`addresslookup.abcd123.eu-west-2.rds.amazonaws.com`|
|address_lookup_rds_database|The name of the database used by this lambda|`addressbasepremium`|
|address_lookup_rds_ingest_user|The `step function` user that will ingest the data|`addresslookupingester`|
|address_lookup_rds_ingest_password|The `step function` user password|`???`|
|address_lookup_rds_admin_user|The `rds` `master user` used for initial setup of application user|`root`|
|address_lookup_rds_admin_password|The `rds` `master password` used for initial setup of application user|`???`|


### License

This code is open source software licensed under the [Apache 2.0 License]("http://www.apache.org/licenses/LICENSE-2.0.html").
