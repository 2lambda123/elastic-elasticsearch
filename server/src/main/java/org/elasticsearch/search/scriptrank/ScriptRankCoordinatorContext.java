/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.scriptrank;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.action.search.SearchPhaseController;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rank.RankCoordinatorContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ScriptRankCoordinatorContext extends RankCoordinatorContext {

    private final ScriptService scriptService;
    private final Script script;

    private List<PriorityQueue<ScoreDoc>> queues = new ArrayList<>();

    public ScriptRankCoordinatorContext(int size, int from, int windowSize, ScriptService scriptService, Script script) {
        super(size, from, windowSize);
        this.scriptService = scriptService;
        this.script = script;
    }

    /**
     * @param querySearchResults Each QuerySearchResults contains an internal list of retriever results for a given query.
     *                           The outer list is per shard, inner is per retriever.
     * @param topDocStats
     * @return
     */
    @Override
    public SearchPhaseController.SortedTopDocs rank(
        List<QuerySearchResult> querySearchResults,
        SearchPhaseController.TopDocsStats topDocStats
    ) {
        for (QuerySearchResult querySearchResult : querySearchResults) {
            // this is the results for each retriever, the whole thing is for an individual shard.
            var topDocsList = ((ScriptRankShardResult) querySearchResult.getRankShardResult()).getTopDocsList();

            if (queues.isEmpty()) {
                for (int i = 0; i < topDocsList.size(); ++i) {
                    queues.add(new PriorityQueue<>(windowSize + from) {
                        @Override
                        protected boolean lessThan(ScoreDoc a, ScoreDoc b) {
                            float score1 = a.score;
                            float score2 = b.score;
                            if (score1 != score2) {
                                return score1 < score2;
                            }
                            if (a.shardIndex != b.shardIndex) {
                                return a.shardIndex > b.shardIndex;
                            }
                            return a.doc > b.doc;
                        }
                    });
                }
            }

            // Each result in the topDocsList corresponds to a retriever. The whole thing is for 1 shard.
            for (int i = 0; i < topDocsList.size(); ++i) {
                for (ScoreDoc scoreDoc : topDocsList.get(i).scoreDocs) {
                    scoreDoc.shardIndex = querySearchResult.getShardIndex();
                    queues.get(i).add(scoreDoc);
                }
            }
        }

        var seen = new HashMap<RankKey, ScoreDoc>();

        for (PriorityQueue<ScoreDoc> priorityQueue : queues) {
            for (ScoreDoc scoreDoc : priorityQueue) {
                seen.putIfAbsent(new RankKey(scoreDoc.doc, scoreDoc.shardIndex), scoreDoc);
            }
        }
        topDocStats.fetchHits = seen.size();

        return new SearchPhaseController.SortedTopDocs(seen.values().toArray(ScoreDoc[]::new), false, null, null, null, 0);
    }

    @Override
    @SuppressWarnings("unchecked")
    public SearchHits getHits(
        SearchPhaseController.ReducedQueryPhase reducedQueryPhase,
        AtomicArray<? extends SearchPhaseResult> fetchResultsArray
    ) {
        RankScript.Factory factory = scriptService.compile(script, RankScript.CONTEXT);
        RankScript rankScript = factory.newInstance(script.getParams());


// Example that calculates RRF
/*
def results = [:];
for (def retrieverResult : docs) {
    int index = retrieverResult.size();
    for (ScriptRankDoc scriptRankDoc : retrieverResult) {
        ScoreDoc scoreDoc = scriptRankDoc.scoreDoc();
        String kwField = scriptRankDoc.fields().get(\"kw\");
        results.compute(
                new RankKey(scoreDoc.doc, scoreDoc.shardIndex),
                (key, value) -> {
                    def v = value;
                    if (v == null) {
                        v = new ScoreDoc(scoreDoc.doc, 0f, scoreDoc.shardIndex);
                    }
                    v.score += 1.0f / (60 + index );
                    return v;
                }
        );
        --index;
    }
}
def output = new ArrayList(results.values());
output.sort((ScoreDoc sd1, ScoreDoc sd2) -> { return sd1.score < sd2.score ? 1 : -1; });
return output;

*/

        record LookupData(Map<String, Object> fields, float[] queryScores) {
        }

        Map<RankKey, LookupData> lookup = new HashMap<>();
        for (var fetchResult : fetchResultsArray.asList()) {
            for (var hit : fetchResult.fetchResult().hits().getHits()) {
                LogManager.getLogger(ScriptRankCoordinatorContext.class).info("RANK_KEY ID ["+ hit.docId() + "] SHARD [" + fetchResult.getShardIndex() + "]");
                lookup.put(
                    new RankKey(hit.docId(), fetchResult.getShardIndex()),
                    new LookupData(
                        ((ScriptRankHitData) hit.getRankHitData()).getFieldData(),
                        ((ScriptRankHitData) hit.getRankHitData()).getQueryScores()
                    )
                );
            }
        }

        List<List<ScriptRankDoc>> allRetrieverResults = new ArrayList<>(queues.size());
        for (PriorityQueue<ScoreDoc> queue : queues) {
            List<ScriptRankDoc> currentRetrieverResults = new ArrayList<>();
            while (queue.size() != 0) {
                ScoreDoc scoreDoc = queue.pop();
                LogManager.getLogger(ScriptRankCoordinatorContext.class).info("SCORE_DOC ID ["+ scoreDoc.doc + "] SHARD [" + scoreDoc.shardIndex + "]");
                var lookupData = lookup.get(new RankKey(scoreDoc.doc, scoreDoc.shardIndex));
                currentRetrieverResults.add(
                    new ScriptRankDoc(
                        scoreDoc,
                        lookupData.fields,
                        lookupData.queryScores
                    )
                );
            }
            allRetrieverResults.add(currentRetrieverResults);
        }

        List<ScoreDoc> scriptResult = rankScript.execute(allRetrieverResults);

        var sortedTopDocs = new SearchPhaseController.SortedTopDocs(scriptResult.toArray(ScoreDoc[]::new), false, null, null, null, 0);
        var updatedReducedQueryPhase = new SearchPhaseController.ReducedQueryPhase(
            reducedQueryPhase.totalHits(),
            scriptResult.size(),
            reducedQueryPhase.maxScore(),
            reducedQueryPhase.timedOut(),
            reducedQueryPhase.terminatedEarly(),
            reducedQueryPhase.suggest(),
            reducedQueryPhase.aggregations(),
            reducedQueryPhase.profileBuilder(),
            sortedTopDocs,
            reducedQueryPhase.sortValueFormats(),
            this,
            reducedQueryPhase.numReducePhases(),
            reducedQueryPhase.size(),
            reducedQueryPhase.from(),
            reducedQueryPhase.isEmptyResult()
        );

        return SearchPhaseController.getHits(updatedReducedQueryPhase, false, fetchResultsArray);
    }
}
