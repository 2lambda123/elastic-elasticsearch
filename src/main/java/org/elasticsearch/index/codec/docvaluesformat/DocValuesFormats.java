/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.codec.docvaluesformat;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMap;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.lucene.Lucene;

/**
 * This class represents the set of Elasticsearch "built-in"
 * {@link DocValuesFormatProvider.Factory doc values format factories}
 */
public class DocValuesFormats {

    private static final ImmutableMap<String, PreBuiltDocValuesFormatProvider.Factory> builtInDocValuesFormats;

    static {
        assert Version.LUCENE_44.onOrAfter(Lucene.VERSION) : "change memory to point to the new Lucene 4.5 MemoryDVF";
        MapBuilder<String, PreBuiltDocValuesFormatProvider.Factory> builtInDocValuesFormatsX = MapBuilder.newMapBuilder();
        for (String name : DocValuesFormat.availableDocValuesFormats()) {
            builtInDocValuesFormatsX.put(name, new PreBuiltDocValuesFormatProvider.Factory(DocValuesFormat.forName(name)));
        }
        // LUCENE UPGRADE: update those DVF if necessary
        builtInDocValuesFormatsX.put("default", new PreBuiltDocValuesFormatProvider.Factory("default", DocValuesFormat.forName("Lucene42")));
        builtInDocValuesFormatsX.put("memory", new PreBuiltDocValuesFormatProvider.Factory("memory", DocValuesFormat.forName("Lucene42")));
        builtInDocValuesFormatsX.put("disk", new PreBuiltDocValuesFormatProvider.Factory("disk", DocValuesFormat.forName("Disk")));
        builtInDocValuesFormats = builtInDocValuesFormatsX.immutableMap();
    }

    public static DocValuesFormatProvider.Factory getAsFactory(String name) {
        return builtInDocValuesFormats.get(name);
    }

    public static DocValuesFormatProvider getAsProvider(String name) {
        final PreBuiltDocValuesFormatProvider.Factory factory = builtInDocValuesFormats.get(name);
        return factory == null ? null : factory.get();
    }

    public static ImmutableCollection<PreBuiltDocValuesFormatProvider.Factory> listFactories() {
        return builtInDocValuesFormats.values();
    }
}
