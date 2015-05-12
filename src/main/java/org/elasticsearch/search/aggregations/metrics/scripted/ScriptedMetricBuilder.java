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

package org.elasticsearch.search.aggregations.metrics.scripted;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptParameterParser;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregationBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * Builder for the {@link ScriptedMetric} aggregation.
 */
public class ScriptedMetricBuilder extends MetricsAggregationBuilder {

    private Script initScript = null;
    private Script mapScript = null;
    private Script combineScript = null;
    private Script reduceScript = null;
    private Map<String, Object> params = null;
    @Deprecated
    private Map<String, Object> reduceParams = null;
    @Deprecated
    private String initScriptString = null;
    @Deprecated
    private String mapScriptString = null;
    @Deprecated
    private String combineScriptString = null;
    @Deprecated
    private String reduceScriptString = null;
    @Deprecated
    private String initScriptFile = null;
    @Deprecated
    private String mapScriptFile = null;
    @Deprecated
    private String combineScriptFile = null;
    @Deprecated
    private String reduceScriptFile = null;
    @Deprecated
    private String initScriptId = null;
    @Deprecated
    private String mapScriptId = null;
    @Deprecated
    private String combineScriptId = null;
    @Deprecated
    private String reduceScriptId = null;
    @Deprecated
    private String lang = null;

    /**
     * Sole constructor.
     */
    public ScriptedMetricBuilder(String name) {
        super(name, InternalScriptedMetric.TYPE.name());
    }

    /**
     * Set the <tt>init</tt> script.
     */
    public ScriptedMetricBuilder initScript(Script initScript) {
        this.initScript = initScript;
        return this;
    }

    /**
     * Set the <tt>map</tt> script.
     */
    public ScriptedMetricBuilder mapScript(Script mapScript) {
        this.mapScript = mapScript;
        return this;
    }

    /**
     * Set the <tt>combine</tt> script.
     */
    public ScriptedMetricBuilder combineScript(Script combineScript) {
        this.combineScript = combineScript;
        return this;
    }

    /**
     * Set the <tt>reduce</tt> script.
     */
    public ScriptedMetricBuilder reduceScript(Script reduceScript) {
        this.reduceScript = reduceScript;
        return this;
    }

    /**
     * Set parameters that will be available in the <tt>init</tt>, <tt>map</tt>
     * and <tt>combine</tt> phases.
     */
    public ScriptedMetricBuilder params(Map<String, Object> params) {
        this.params = params;
        return this;
    }

    /**
     * Set parameters that will be available in the <tt>reduce</tt> phase.
     * 
     * @deprecated Use {@link #reduceScript(Script)} instead.
     */
    @Deprecated
    public ScriptedMetricBuilder reduceParams(Map<String, Object> reduceParams) {
        this.reduceParams = reduceParams;
        return this;
    }

    /**
     * Set the <tt>init</tt> script.
     * 
     * @deprecated Use {@link #initScript(Script)} instead.
     */
    @Deprecated
    public ScriptedMetricBuilder initScript(String initScript) {
        this.initScriptString = initScript;
        return this;
    }

    /**
     * Set the <tt>map</tt> script.
     * 
     * @deprecated Use {@link #mapScript(Script)} instead.
     */
    @Deprecated
    public ScriptedMetricBuilder mapScript(String mapScript) {
        this.mapScriptString = mapScript;
        return this;
    }

    /**
     * Set the <tt>combine</tt> script.
     * 
     * @deprecated Use {@link #combineScript(Script)} instead.
     */
    @Deprecated
    public ScriptedMetricBuilder combineScript(String combineScript) {
        this.combineScriptString = combineScript;
        return this;
    }

    /**
     * Set the <tt>reduce</tt> script.
     * 
     * @deprecated Use {@link #reduceScript(Script)} instead.
     */
    @Deprecated
    public ScriptedMetricBuilder reduceScript(String reduceScript) {
        this.reduceScriptString = reduceScript;
        return this;
    }

    /**
     * Set the <tt>init</tt> script file.
     * 
     * @deprecated Use {@link #initScript(Script)} instead.
     */
    @Deprecated
    public ScriptedMetricBuilder initScriptFile(String initScriptFile) {
        this.initScriptFile = initScriptFile;
        return this;
    }

    /**
     * Set the <tt>map</tt> script file.
     * 
     * @deprecated Use {@link #mapScript(Script)} instead.
     */
    @Deprecated
    public ScriptedMetricBuilder mapScriptFile(String mapScriptFile) {
        this.mapScriptFile = mapScriptFile;
        return this;
    }

    /**
     * Set the <tt>combine</tt> script file.
     * 
     * @deprecated Use {@link #combineScript(Script)} instead.
     */
    @Deprecated
    public ScriptedMetricBuilder combineScriptFile(String combineScriptFile) {
        this.combineScriptFile = combineScriptFile;
        return this;
    }

    /**
     * Set the <tt>reduce</tt> script file.
     * 
     * @deprecated Use {@link #reduceScript(Script)} instead.
     */
    @Deprecated
    public ScriptedMetricBuilder reduceScriptFile(String reduceScriptFile) {
        this.reduceScriptFile = reduceScriptFile;
        return this;
    }

    /**
     * Set the indexed <tt>init</tt> script id.
     * 
     * @deprecated Use {@link #initScript(Script)} instead.
     */
    @Deprecated
    public ScriptedMetricBuilder initScriptId(String initScriptId) {
        this.initScriptId = initScriptId;
        return this;
    }

    /**
     * Set the indexed <tt>map</tt> script id.
     * 
     * @deprecated Use {@link #mapScript(Script)} instead.
     */
    @Deprecated
    public ScriptedMetricBuilder mapScriptId(String mapScriptId) {
        this.mapScriptId = mapScriptId;
        return this;
    }

    /**
     * Set the indexed <tt>combine</tt> script id.
     * 
     * @deprecated Use {@link #combineScript(Script)} instead.
     */
    @Deprecated
    public ScriptedMetricBuilder combineScriptId(String combineScriptId) {
        this.combineScriptId = combineScriptId;
        return this;
    }

    /**
     * Set the indexed <tt>reduce</tt> script id.
     * 
     * @deprecated Use {@link #reduceScript(Script)} instead.
     */
    @Deprecated
    public ScriptedMetricBuilder reduceScriptId(String reduceScriptId) {
        this.reduceScriptId = reduceScriptId;
        return this;
    }

    /**
     * Set the script language.
     * 
     * @deprecated Use {@link #initScript(Script)}, {@link #mapScript(Script)},
     *             {@link #combineScript(Script)}, and
     *             {@link #reduceScript(Script)} instead.
     */
    @Deprecated
    public ScriptedMetricBuilder lang(String lang) {
        this.lang = lang;
        return this;
    }

    @Override
    protected void internalXContent(XContentBuilder builder, Params builderParams) throws IOException {

        if (initScript != null) {
            builder.field(ScriptedMetricParser.INIT_SCRIPT_FIELD.getPreferredName(), initScript);
        }

        if (mapScript != null) {
            builder.field(ScriptedMetricParser.MAP_SCRIPT_FIELD.getPreferredName(), mapScript);
        }

        if (combineScript != null) {
            builder.field(ScriptedMetricParser.COMBINE_SCRIPT_FIELD.getPreferredName(), combineScript);
        }

        if (reduceScript != null) {
            builder.field(ScriptedMetricParser.REDUCE_SCRIPT_FIELD.getPreferredName(), reduceScript);
        }

        if (params != null) {
            builder.field(ScriptedMetricParser.PARAMS_FIELD.getPreferredName());
            builder.map(params);
        }
        
        if (reduceParams != null) {
            builder.field(ScriptedMetricParser.REDUCE_PARAMS_FIELD.getPreferredName());
            builder.map(reduceParams);
        }
        
        if (initScriptString != null) {
            builder.field(ScriptedMetricParser.INIT_SCRIPT, initScriptString);
        }
        
        if (mapScriptString != null) {
            builder.field(ScriptedMetricParser.MAP_SCRIPT, mapScriptString);
        }
        
        if (combineScriptString != null) {
            builder.field(ScriptedMetricParser.COMBINE_SCRIPT, combineScriptString);
        }
        
        if (reduceScriptString != null) {
            builder.field(ScriptedMetricParser.REDUCE_SCRIPT, reduceScriptString);
        }
        
        if (initScriptFile != null) {
            builder.field(ScriptedMetricParser.INIT_SCRIPT + ScriptParameterParser.FILE_SUFFIX, initScriptFile);
        }
        
        if (mapScriptFile != null) {
            builder.field(ScriptedMetricParser.MAP_SCRIPT + ScriptParameterParser.FILE_SUFFIX, mapScriptFile);
        }
        
        if (combineScriptFile != null) {
            builder.field(ScriptedMetricParser.COMBINE_SCRIPT + ScriptParameterParser.FILE_SUFFIX, combineScriptFile);
        }
        
        if (reduceScriptFile != null) {
            builder.field(ScriptedMetricParser.REDUCE_SCRIPT + ScriptParameterParser.FILE_SUFFIX, reduceScriptFile);
        }
        
        if (initScriptId != null) {
            builder.field(ScriptedMetricParser.INIT_SCRIPT + ScriptParameterParser.INDEXED_SUFFIX, initScriptId);
        }
        
        if (mapScriptId != null) {
            builder.field(ScriptedMetricParser.MAP_SCRIPT + ScriptParameterParser.INDEXED_SUFFIX, mapScriptId);
        }
        
        if (combineScriptId != null) {
            builder.field(ScriptedMetricParser.COMBINE_SCRIPT + ScriptParameterParser.INDEXED_SUFFIX, combineScriptId);
        }
        
        if (reduceScriptId != null) {
            builder.field(ScriptedMetricParser.REDUCE_SCRIPT + ScriptParameterParser.INDEXED_SUFFIX, reduceScriptId);
        }
        
        if (lang != null) {
            builder.field(ScriptedMetricParser.LANG_FIELD.getPreferredName(), lang);
        }
    }

}
