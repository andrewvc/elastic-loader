# elastic-loader

A utility for bulk loading data into elasticsearch. Loads its own `.eloader` format for elasticsearch data. It can also execute multiple commands in a simplified format. It's a single java jar with no external dependencies so it's easy to run.

## Download

Current Version: [elastic-loader-0.3.0.jar](http://elastic-loader.s3.amazonaws.com/elastic-loader-0.3.0.jar)

## Usage

The basic usage is `java -jar elastic-loader.jar SERVER_URL [FILENAME]`. The filename is optional, as input may also be read via STDIN.

For example, you could run `java -jar elastic-loader.jar http://localhost:9200 test_import.txt` to load the data from the file `test_import.txt` into the server at `http://localhost:9200`. Where `test_import` looks like:

```
# Issue an HTTP Request, without stopping due to failure
TRY DELETE /foo
# Issue an HTTP request that will stop the import if it fails
POST /foo {"_mapping": {}}
# Bulk index items of type bar into the foo index
BULK INDEX foo/bar
{"user": "foo", "_id": 1}
{"user": "baz", "_id": 3}
# Bulk index items of type baz into the foo index
BULK INDEX foo/baz
{"ohai": "bort"}
{"ohai": "there", "_id": 1}
DELETE /foo/baz/1
```

The format allows you to issue arbitrary HTTP requests and to use the bulk API to add new documents as well with a more concise format. You specify the index and document type once, and subsequent documents will be imported. Each document should probably specify the `_id` field unless you truly want random IDs from elasticsearch.

Feel free to omit the filename and pipe data through STDIN as well. You could run `cat test_import.txt | java -jar elastic-loader.jar` as well.

This project was created to aid the process of running examples for the book [Exploring Elasticsearch](http://exploring-elasticsearch.com).

## License

Copyright © 2013 Andrew Cholakian

Distributed under the Eclipse Public License, the same as Clojure.
