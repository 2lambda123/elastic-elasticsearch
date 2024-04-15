/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.doc;

class DocTestUtils {
    public static Map<String,String> SAMPLE_TEST_DOCS = Map.of(
        "mapping-charfilter.asciidoc", """
[[mapper-annotated-text]]
=== Mapper annotated text plugin

experimental[]

The mapper-annotated-text plugin provides the ability to index text that is a
combination of free-text and special markup that is typically used to identify
items of interest such as people or organisations (see NER or Named Entity Recognition
tools).


The elasticsearch markup allows one or more additional tokens to be injected, unchanged, into the token
stream at the same position as the underlying text it annotates.

:plugin_name: mapper-annotated-text
include::install_remove.asciidoc[]

[[mapper-annotated-text-usage]]
==== Using the `annotated-text` field

The `annotated-text` tokenizes text content as per the more common {ref}/text.html[`text`] field (see
"limitations" below) but also injects any marked-up annotation tokens directly into
the search index:

[source,console]
--------------------------
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "my_field": {
        "type": "annotated_text"
      }
    }
  }
}
--------------------------

Such a mapping would allow marked-up text eg wikipedia articles to be indexed as both text
and structured tokens. The annotations use a markdown-like syntax using URL encoding of
one or more values separated by the `&` symbol.


We can use the "_analyze" api to test how an example annotation would be stored as tokens
in the search index:


[source,js]
--------------------------
GET my-index-000001/_analyze
{
  "field": "my_field",
  "text":"Investors in [Apple](Apple+Inc.) rejoiced."
}
--------------------------
// NOTCONSOLE

Response:

[source,js]
--------------------------------------------------
{
  "tokens": [
    {
      "token": "investors",
      "start_offset": 0,
      "end_offset": 9,
      "type": "<ALPHANUM>",
      "position": 0
    },
    {
      "token": "in",
      "start_offset": 10,
      "end_offset": 12,
      "type": "<ALPHANUM>",
      "position": 1
    },
    {
      "token": "Apple Inc.", <1>
      "start_offset": 13,
      "end_offset": 18,
      "type": "annotation",
      "position": 2
    },
    {
      "token": "apple",
      "start_offset": 13,
      "end_offset": 18,
      "type": "<ALPHANUM>",
      "position": 2
    },
    {
      "token": "rejoiced",
      "start_offset": 19,
      "end_offset": 27,
      "type": "<ALPHANUM>",
      "position": 3
    }
  ]
}
--------------------------------------------------
// NOTCONSOLE

<1> Note the whole annotation token `Apple Inc.` is placed, unchanged as a single token in
the token stream and at the same position (position 2) as the text token (`apple`) it annotates.


We can now perform searches for annotations using regular `term` queries that don't tokenize
the provided search values. Annotations are a more precise way of matching as can be seen
in this example where a search for `Beck` will not match `Jeff Beck` :

[source,console]
--------------------------
# Example documents
PUT my-index-000001/_doc/1
{
  "my_field": "[Beck](Beck) announced a new tour"<1>
}

PUT my-index-000001/_doc/2
{
  "my_field": "[Jeff Beck](Jeff+Beck&Guitarist) plays a strat"<2>
}

# Example search
GET my-index-000001/_search
{
  "query": {
    "term": {
        "my_field": "Beck" <3>
    }
  }
}
--------------------------

<1> As well as tokenising the plain text into single words e.g. `beck`, here we
inject the single token value `Beck` at the same position as `beck` in the token stream.
<2> Note annotations can inject multiple tokens at the same position - here we inject both
the very specific value `Jeff Beck` and the broader term `Guitarist`. This enables
broader positional queries e.g. finding mentions of a `Guitarist` near to `strat`.
<3> A benefit of searching with these carefully defined annotation tokens is that a query for
`Beck` will not match document 2 that contains the tokens `jeff`, `beck` and `Jeff Beck`

WARNING: Any use of `=` signs in annotation values eg `[Prince](person=Prince)` will
cause the document to be rejected with a parse failure. In future we hope to have a use for
the equals signs so wil actively reject documents that contain this today.


[[mapper-annotated-text-tips]]
==== Data modelling tips
===== Use structured and unstructured fields

Annotations are normally a way of weaving structured information into unstructured text for
higher-precision search.

`Entity resolution` is a form of document enrichment undertaken by specialist software or people
where references to entities in a document are disambiguated by attaching a canonical ID.
The ID is used to resolve any number of aliases or distinguish between people with the
same name. The hyperlinks connecting Wikipedia's articles are a good example of resolved
entity IDs woven into text.

These IDs can be embedded as annotations in an annotated_text field but it often makes
sense to include them in dedicated structured fields to support discovery via aggregations:

[source,console]
--------------------------
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "my_unstructured_text_field": {
        "type": "annotated_text"
      },
      "my_structured_people_field": {
        "type": "text",
        "fields": {
          "keyword" : {
            "type": "keyword"
          }
        }
      }
    }
  }
}
--------------------------

Applications would then typically provide content and discover it as follows:

[source,console]
--------------------------
# Example documents
PUT my-index-000001/_doc/1
{
  "my_unstructured_text_field": "[Shay](%40kimchy) created elasticsearch",
  "my_twitter_handles": ["@kimchy"] <1>
}

GET my-index-000001/_search
{
  "query": {
    "query_string": {
        "query": "elasticsearch OR logstash OR kibana",<2>
        "default_field": "my_unstructured_text_field"
    }
  },
  "aggregations": {
  \t"top_people" :{
  \t    "significant_terms" : { <3>
\t       "field" : "my_twitter_handles.keyword"
  \t    }
  \t}
  }
}
--------------------------

<1> Note the `my_twitter_handles` contains a list of the annotation values
also used in the unstructured text. (Note the annotated_text syntax requires escaping).
By repeating the annotation values in a structured field this application has ensured that
the tokens discovered in the structured field can be used for search and highlighting
in the unstructured field.
<2> In this example we search for documents that talk about components of the elastic stack
<3> We use the `my_twitter_handles` field here to discover people who are significantly
associated with the elastic stack.

===== Avoiding over-matching annotations
By design, the regular text tokens and the annotation tokens co-exist in the same indexed
field but in rare cases this can lead to some over-matching.

The value of an annotation often denotes a _named entity_ (a person, place or company).
The tokens for these named entities are inserted untokenized, and differ from typical text
tokens because they are normally:

* Mixed case e.g. `Madonna`
* Multiple words e.g. `Jeff Beck`
* Can have punctuation or numbers e.g. `Apple Inc.` or `@kimchy`

This means, for the most part, a search for a named entity in the annotated text field will
not have any false positives e.g. when selecting `Apple Inc.` from an aggregation result
you can drill down to highlight uses in the text without "over matching" on any text tokens
like the word `apple` in this context:

    the apple was very juicy

However, a problem arises if your named entity happens to be a single term and lower-case e.g. the
company `elastic`. In this case, a search on the annotated text field for the token `elastic`
may match a text document such as this:

    they fired an elastic band

To avoid such false matches users should consider prefixing annotation values to ensure
they don't name clash with text tokens e.g.

    [elastic](Company_elastic) released version 7.0 of the elastic stack today




[[mapper-annotated-text-highlighter]]
==== Using the `annotated` highlighter

The `annotated-text` plugin includes a custom highlighter designed to mark up search hits
in a way which is respectful of the original markup:

[source,console]
--------------------------
# Example documents
PUT my-index-000001/_doc/1
{
  "my_field": "The cat sat on the [mat](sku3578)"
}

GET my-index-000001/_search
{
  "query": {
    "query_string": {
        "query": "cats"
    }
  },
  "highlight": {
    "fields": {
      "my_field": {
        "type": "annotated", <1>
        "require_field_match": false
      }
    }
  }
}
--------------------------

<1> The `annotated` highlighter type is designed for use with annotated_text fields

The annotated highlighter is based on the `unified` highlighter and supports the same
settings but does not use the `pre_tags` or `post_tags` parameters. Rather than using
html-like markup such as `<em>cat</em>` the annotated highlighter uses the same
markdown-like syntax used for annotations and injects a key=value annotation where `_hit_term`
is the key and the matched search term is the value e.g.

    The [cat](_hit_term=cat) sat on the [mat](sku3578)

The annotated highlighter tries to be respectful of any existing markup in the original
text:

* If the search term matches exactly the location of an existing annotation then the
`_hit_term` key is merged into the url-like syntax used in the `(...)` part of the
existing annotation.
* However, if the search term overlaps the span of an existing annotation it would break
the markup formatting so the original annotation is removed in favour of a new annotation
with just the search hit information in the results.
* Any non-overlapping annotations in the original text are preserved in highlighter
selections


[[mapper-annotated-text-limitations]]
==== Limitations

The annotated_text field type supports the same mapping settings as the `text` field type
but with the following exceptions:

* No support for `fielddata` or `fielddata_frequency_filter`
* No support for `index_prefixes` or `index_phrases` indexing

""",
        "painless-field-context.asciidoc", """
[[painless-field-context]]
=== Field context

Use a Painless script to create a
{ref}/search-fields.html#script-fields[script field] to return
a customized value for each document in the results of a query.

*Variables*

`params` (`Map`, read-only)::
        User-defined parameters passed in as part of the query.

`doc` (`Map`, read-only)::
        Contains the fields of the specified document where each field is a
        `List` of values.

{ref}/mapping-source-field.html[`params['_source']`] (`Map`, read-only)::
        Contains extracted JSON in a `Map` and `List` structure for the fields
        existing in a stored document.

*Return*

`Object`::
        The customized value for each document.

*API*

Both the standard <<painless-api-reference-shared, Painless API>> and
<<painless-api-reference-field, Specialized Field API>> are available.


*Example*

To run this example, first follow the steps in
<<painless-context-examples, context examples>>.

You can then use these two example scripts to compute custom information
for each search hit and output it to two new fields.

The first script gets the doc value for the `datetime` field and calls
the `getDayOfWeekEnum` function to determine the corresponding day of the week.

[source,Painless]
----
doc['datetime'].value.getDayOfWeekEnum().getDisplayName(TextStyle.FULL, Locale.ROOT)
----

The second script calculates the number of actors. Actors' names are stored
as a keyword array in the `actors` field.

[source,Painless]
----
doc['actors'].size()  <1>
----

<1> By default, doc values are not available for `text` fields. If `actors` was
a `text` field, you could still calculate the number of actors by extracting
values from `_source` with `params['_source']['actors'].size()`.

The following request returns the calculated day of week and the number of
actors that appear in each play:

[source,console]
----
GET seats/_search
{
  "size": 2,
  "query": {
    "match_all": {}
  },
  "script_fields": {
    "day-of-week": {
      "script": {
        "source": "doc['datetime'].value.getDayOfWeekEnum().getDisplayName(TextStyle.FULL, Locale.ROOT)"
      }
    },
    "number-of-actors": {
      "script": {
        "source": "doc['actors'].size()"
      }
    }
  }
}
----
// TEST[setup:seats]

[source,console-result]
----
{
  "took" : 68,
  "timed_out" : false,
  "_shards" : {
    "total" : 1,
    "successful" : 1,
    "skipped" : 0,
    "failed" : 0
  },
  "hits" : {
    "total" : {
      "value" : 11,
      "relation" : "eq"
    },
    "max_score" : 1.0,
    "hits" : [
      {
        "_index" : "seats",
        "_id" : "1",
        "_score" : 1.0,
        "fields" : {
          "day-of-week" : [
            "Thursday"
          ],
          "number-of-actors" : [
            4
          ]
        }
      },
      {
        "_index" : "seats",
        "_id" : "2",
        "_score" : 1.0,
        "fields" : {
          "day-of-week" : [
            "Thursday"
          ],
          "number-of-actors" : [
            1
          ]
        }
      }
    ]
  }
}
----
// TESTRESPONSE[s/"took" : 68/"took" : "\$body.took"/]
""",
        "painless-field-context.mdx", """---
id: enElasticsearchPainlessPainlessFieldContext
slug: /en/elasticsearch/painless/painless-field-context
title: Field context
description: Description to be written
tags: []
---

<div id="painless-field-context"></div>

Use a Painless script to create a
[script field](((ref))/search-fields.html#script-fields) to return
a customized value for each document in the results of a query.

**Variables**

`params` (`Map`, read-only)
    : User-defined parameters passed in as part of the query.

`doc` (`Map`, read-only)
    : Contains the fields of the specified document where each field is a
    `List` of values.

[`params['_source']`](((ref))/mapping-source-field.html) (`Map`, read-only)
    : Contains extracted JSON in a `Map` and `List` structure for the fields
    existing in a stored document.

**Return**

`Object`
    : The customized value for each document.

**API**

Both the standard <DocLink id="enElasticsearchPainlessPainlessApiReferenceShared">Painless API</DocLink> and
<DocLink id="enElasticsearchPainlessPainlessApiReferenceField">Specialized Field API</DocLink> are available.

**Example**

To run this example, first follow the steps in
<DocLink id="enElasticsearchPainlessPainlessContextExamples">context examples</DocLink>.

You can then use these two example scripts to compute custom information
for each search hit and output it to two new fields.

The first script gets the doc value for the `datetime` field and calls
the `getDayOfWeekEnum` function to determine the corresponding day of the week.

```Painless
doc['datetime'].value.getDayOfWeekEnum().getDisplayName(TextStyle.FULL, Locale.ROOT)
```

The second script calculates the number of actors. Actors' names are stored
as a keyword array in the `actors` field.

```Painless
doc['actors'].size()   [^1]
```
[^1]: By default, doc values are not available for `text` fields. If `actors` was
a `text` field, you could still calculate the number of actors by extracting
values from `_source` with `params['_source']['actors'].size()`.

The following request returns the calculated day of week and the number of
actors that appear in each play:

```console
GET seats/_search
{
  "size": 2,
  "query": {
    "match_all": {}
  },
  "script_fields": {
    "day-of-week": {
      "script": {
        "source": "doc['datetime'].value.getDayOfWeekEnum().getDisplayName(TextStyle.FULL, Locale.ROOT)"
      }
    },
    "number-of-actors": {
      "script": {
        "source": "doc['actors'].size()"
      }
    }
  }
}
```
{/* TEST[setup:seats] */}

```console-result
{
  "took" : 68,
  "timed_out" : false,
  "_shards" : {
    "total" : 1,
    "successful" : 1,
    "skipped" : 0,
    "failed" : 0
  },
  "hits" : {
    "total" : {
      "value" : 11,
      "relation" : "eq"
    },
    "max_score" : 1.0,
    "hits" : [
      {
        "_index" : "seats",
        "_id" : "1",
        "_score" : 1.0,
        "fields" : {
          "day-of-week" : [
            "Thursday"
          ],
          "number-of-actors" : [
            4
          ]
        }
      },
      {
        "_index" : "seats",
        "_id" : "2",
        "_score" : 1.0,
        "fields" : {
          "day-of-week" : [
            "Thursday"
          ],
          "number-of-actors" : [
            1
          ]
        }
      }
    ]
  }
}
```
{/* TESTRESPONSE[s/"took" : 68/"took" : "\$body.took"/] */}
"""
    );
}
