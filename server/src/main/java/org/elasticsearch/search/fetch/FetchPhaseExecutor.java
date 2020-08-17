/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.fetch;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FetchPhaseExecutor {

    private final List<FetchSubPhaseExecutor> executors = new ArrayList<>();

    public FetchPhaseExecutor(FetchSubPhase[] fetchSubPhases, SearchContext context) throws IOException {
        for (FetchSubPhase subPhase : fetchSubPhases) {
            FetchSubPhaseExecutor spe = subPhase.getExecutor(context);
            if (spe != null) {
                executors.add(spe);
            }
        }
    }

    public void setNextReader(LeafReaderContext currentReaderContext) throws IOException {
        for (FetchSubPhaseExecutor spe : executors) {
            spe.setNextReader(currentReaderContext);
        }
    }

    public void execute(FetchSubPhase.HitContext hitContext) throws IOException {
        for (FetchSubPhaseExecutor spe : executors) {
            spe.execute(hitContext);
        }
    }
}
