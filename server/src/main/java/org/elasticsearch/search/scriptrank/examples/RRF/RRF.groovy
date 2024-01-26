/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

import org.apache.lucene.search.ScoreDoc
import org.elasticsearch.search.scriptrank.RankKey
import org.elasticsearch.search.scriptrank.ScriptRankDoc

def results = [:];
for (def retrieverResult : docs) {
    int index = retrieverResult.size();
    for (ScriptRankDoc scriptRankDoc : retrieverResult) {
        ScoreDoc scoreDoc = scriptRankDoc.scoreDoc();
        results.compute(
                new RankKey(scoreDoc.doc, scoreDoc.shardIndex),
                (key, value) -> {
                    def v = value;
                    if (v == null) {
                        v = new ScoreDoc(scoreDoc.doc, 0f, scoreDoc.shardIndex);
                    }
                    v.score += 1.0f / (60 + index);
                    return v;
                }
        );
        --index;
    }
}
def output = new ArrayList(results.values());
output.sort((ScoreDoc sd1, ScoreDoc sd2) -> { return sd1.score < sd2.score ? 1 : -1; });
return output;