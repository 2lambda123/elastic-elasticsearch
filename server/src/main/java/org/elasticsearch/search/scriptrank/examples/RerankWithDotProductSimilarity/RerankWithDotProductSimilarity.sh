#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License
# 2.0 and the Server Side Public License, v 1; you may not use this file except
# in compliance with, at your election, the Elastic License 2.0 or the Server
# Side Public License, v 1.
#

curl -X GET -u elastic:password "localhost:9200/demo/_search?pretty" -H 'Content-Type: application/json' -d'
{
  "_source": false,
  "fields":["kw","v"],
  "retriever": {
    "script_rank": {
      "fields": ["v"],
      "window_size": 10,
      "script": {
          "source": "def output = [];float[] queryVector = new float[params.queryVector.size()];for (int i = 0; i < queryVector.length; ++i) {    queryVector[i] = (float) params.queryVector[i];}queryVector = VectorUtil.l2normalize(queryVector);for (ScriptRankDoc scriptRankDoc : inputs[0]) {    def inputVector = scriptRankDoc.fields()[\"v\"];    float[] docVector = new float[inputVector.size()];    for (int i = 0; i < queryVector.length; ++i) {        docVector[i] = (float) inputVector[i];    }    docVector = VectorUtil.l2normalize(docVector);    float newScore = VectorUtil.dotProduct(queryVector, docVector);            output.add(new ScoreDoc(                    scriptRankDoc.scoreDoc().doc,                    newScore,                    scriptRankDoc.scoreDoc().shardIndex            )) ;}output.sort((ScoreDoc sd1, ScoreDoc sd2) -> { return sd1.score < sd2.score ? 1 : -1; });return output;",
          "params": {
              "queryVector": [2.0, 2.0]
          }
      },
      "retrievers": [
        {
          "standard": {
            "query": {
              "bool": {
                "should": [
                  {
                    "term": {
                      "kw": {
                        "value": "one"
                      }
                    }
                  },
                  {
                    "term": {
                      "kw": {
                        "value": "two"
                      }
                    }
                  },
                  {
                    "term": {
                      "kw": {
                        "value": "three"
                      }
                    }
                  }
                ]
              }
            }
          }
        }
      ]
    }
  }
}
'
