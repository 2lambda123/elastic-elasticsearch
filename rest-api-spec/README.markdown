# Elasticsearch REST API JSON specification

This repository contains a collection of JSON files which describe the [Elasticsearch](http://elastic.co) HTTP API.

Their purpose is to formalize and standardize the API, to facilitate development of libraries and integrations.

Example for the ["Create Index"](http://www.elastic.co/guide/en/elasticsearch/reference/master/indices-create-index.html) API:

```json
{
  "indices.create": {
    "documentation": "http://www.elastic.co/guide/en/elasticsearch/reference/master/indices-create-index.html",
    "stability": "stable",
    "methods": ["PUT", "POST"],
    "url": {
      "path": "/{index}",
      "paths": ["/{index}"],
      "parts": {
        "index": {
          "type" : "string",
          "required" : true,
          "description" : "The name of the index"
        }
      },
      "params": {
        "timeout": {
          "type" : "time",
          "description" : "Explicit operation timeout"
        }
      }
    },
    "body": {
      "description" : "The configuration for the index (`settings` and `mappings`)"
    }
  }
}
```

The specification contains:

* The _name_ of the API (`indices.create`), which usually corresponds to the client calls
* Link to the documentation at <http://elastic.co>
* `stability` indicating the state of the API, has to be declared explicitly or YAML tests will fail
    * `private` this API should not be be implemented by clients
    * `experimental` highly likely to break in the near future (minor/path), no bwc guarantees. 
    Possibly removed in the future.
    * `beta` less likely to break or be removed but still reserve the right to do so
    * `internal` API that is stable with regards to its lifetime (here to stay) but makes no guarantees towards its
     output format. Typically used by monitoring API's exposing internal representations.
    * `stable` No backwards breaking changes in a minor (default if not specified)
* List of HTTP methods for the endpoint
* URL specification: path, parts, parameters
* Whether body is allowed for the endpoint or not and its description


The `methods` and `url.paths` elements list all possible HTTP methods and URLs for the endpoint;
it is the responsibility of the developer to use this information for a sensible API on the target platform.

## License

This software is licensed under the Apache License, version 2 ("ALv2").
