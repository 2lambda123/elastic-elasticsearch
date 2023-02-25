/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank.rrf;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.rank.RankShardResult;

import java.io.IOException;
import java.util.Objects;

public class RRFRankShardResult implements RankShardResult {

    public final int queryCount;
    public final RRFRankDoc[] rrfRankDocs;

    public RRFRankShardResult(int queryCount, RRFRankDoc[] rrfRankDocs) {
        this.queryCount = queryCount;
        this.rrfRankDocs = Objects.requireNonNull(rrfRankDocs);
    }

    public RRFRankShardResult(StreamInput in) throws IOException {
        queryCount = in.readVInt();
        rrfRankDocs = in.readArray(RRFRankDoc::new, RRFRankDoc[]::new);
    }

    @Override
    public String getWriteableName() {
        return RRFRankContextBuilder.NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(queryCount);
        out.writeArray(rrfRankDocs);
    }
}
