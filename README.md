
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
### License

This code is open source software licensed under the [Apache 2.0 License]("http://www.apache.org/licenses/LICENSE-2.0.html").
