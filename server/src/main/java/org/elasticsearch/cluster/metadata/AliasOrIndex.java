/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.collect.Tuple;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Encapsulates the  {@link IndexMetaData} instances of a concrete index or indices an alias is pointing to.
 */
public interface AliasOrIndex {

    /**
     * @return whether this an alias or concrete index
     */
    boolean isAlias();

    /**
     * @return All {@link IndexMetaData} of all concrete indices this alias is referring to or if this is a concrete index its {@link IndexMetaData}
     */
    Collection<IndexMetaData> getIndices();

    /**
     * Represents an concrete index and encapsulates its {@link IndexMetaData}
     */
    class Index implements AliasOrIndex {

        private final IndexMetaData concreteIndex;

        public Index(IndexMetaData indexMetaData) {
            this.concreteIndex = indexMetaData;
        }

        @Override
        public boolean isAlias() {
            return false;
        }

        @Override
        public Collection<IndexMetaData> getIndices() {
            return Collections.singletonList(concreteIndex);
        }

        /**
         * @return If this is an concrete index, its {@link IndexMetaData}
         */
        public IndexMetaData getIndex() {
            return concreteIndex;
        }

    }

    /**
     * Represents an alias and groups all {@link IndexMetaData} instances sharing the same alias name together.
     */
    class Alias implements AliasOrIndex {

        private final String aliasName;
        private final SortedMap<String, IndexMetaData> referenceIndexMetaDatas;

        public Alias(AliasMetaData aliasMetaData, IndexMetaData indexMetaData) {
            this.aliasName = aliasMetaData.getAlias();
            this.referenceIndexMetaDatas = new TreeMap<>();
            this.referenceIndexMetaDatas.put(indexMetaData.getIndex().getName(), indexMetaData);
        }

        @Override
        public boolean isAlias() {
            return true;
        }

        @Override
        public Collection<IndexMetaData> getIndices() {
            return referenceIndexMetaDatas.values();
        }

        /**
         * Returns the unique alias metadata per concrete index.
         *
         * (note that although alias can point to the same concrete indices, each alias reference may have its own routing
         * and filters)
         */
        public Iterable<Tuple<String, AliasMetaData>> getConcreteIndexAndAliasMetaDatas() {
            Iterator<IndexMetaData> indexMetaDataIterator = referenceIndexMetaDatas.values().iterator();
            return new Iterable<Tuple<String, AliasMetaData>>() {
                @Override
                public Iterator<Tuple<String, AliasMetaData>> iterator() {
                    return new Iterator<Tuple<String,AliasMetaData>>() {
                        @Override
                        public boolean hasNext() {
                            return indexMetaDataIterator.hasNext();
                        }

                        @Override
                        public Tuple<String, AliasMetaData> next() {
                            IndexMetaData indexMetaData = indexMetaDataIterator.next();
                            return new Tuple<>(indexMetaData.getIndex().getName(), indexMetaData.getAliases().get(aliasName));
                        }

                        @Override
                        public void remove() {
                            throw new UnsupportedOperationException();
                        }
                    };
                }
            };
        }

        public AliasMetaData getFirstAliasMetaData() {
            return getIndices().iterator().next().getAliases().get(aliasName);
        }

        void addIndex(IndexMetaData indexMetaData) {
            this.referenceIndexMetaDatas.put(indexMetaData.getIndex().getName(), indexMetaData);
        }

        void removeIndex(IndexMetaData indexMetaData) {
            String indexName = indexMetaData.getIndex().getName();
            this.referenceIndexMetaDatas.remove(indexName);
        }
    }
}
