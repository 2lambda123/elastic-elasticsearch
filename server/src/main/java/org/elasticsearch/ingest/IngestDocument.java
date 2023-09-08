/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.IndexFieldMapper;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.VersionFieldMapper;
import org.elasticsearch.script.CtxMap;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.script.field.FailFastWriteField;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Represents a single document being captured before indexing and holds the source and metadata (like id, type and index).
 */
public final class IngestDocument {

    public static final String INGEST_KEY = "_ingest";
    public static final String SOURCE_KEY = SourceFieldMapper.NAME; // "_source"
    private static final String INGEST_KEY_PREFIX = INGEST_KEY + ".";
    private static final String SOURCE_PREFIX = SOURCE_KEY + ".";

    public static final String PIPELINE_CYCLE_ERROR_MESSAGE = "Cycle detected for pipeline: ";
    static final String TIMESTAMP = "timestamp";

    private final IngestCtxMap ctxMap;
    private final Map<String, Object> ingestMetadata;

    /**
     * Shallowly read-only, very limited, map-like view of the ctxMap and ingestMetadata,
     * for providing as a model to TemplateScript and ValueSource instances. This avoids the cost of
     * constructing a purpose-built map on each template evaluation.
     */
    private final DelegatingMapView templateModel;

    // Contains all pipelines that have been executed for this document
    private final Set<String> executedPipelines = new LinkedHashSet<>();

    /**
     * An ordered set of the values of the _index that have been used for this document.
     * <p>
     * IMPORTANT: This is only updated after a top-level pipeline has run (see {@code IngestService#executePipelines(...)}).
     * <p>
     * For example, if a processor changes the _index for a document from 'foo' to 'bar',
     * and then another processor changes the value back to 'foo', then the overall effect
     * of the pipeline was that the _index value did not change and so only 'foo' would appear
     * in the index history.
     */
    private Set<String> indexHistory = new LinkedHashSet<>();

    private boolean doNoSelfReferencesCheck = false;
    private boolean reroute = false;

    public IngestDocument(String index, String id, long version, String routing, VersionType versionType, Map<String, Object> source) {
        this.ctxMap = new IngestCtxMap(index, id, version, routing, versionType, ZonedDateTime.now(ZoneOffset.UTC), source);
        this.ingestMetadata = new HashMap<>();
        this.ingestMetadata.put(TIMESTAMP, ctxMap.getMetadata().getNow());
        this.templateModel = initializeTemplateModel();

        // initialize the index history by putting the current index into it
        this.indexHistory.add(index);
    }

    // note: these rest of these constructors deal with the data-centric view of the IngestDocument, not the execution-centric view.
    // For example, the copy constructor doesn't populate the `executedPipelines` or `indexHistory` (as well as some other fields),
    // because those fields are execution-centric.

    /**
     * Copy constructor that creates a new {@link IngestDocument} which has exactly the same properties as the one provided.
     */
    public IngestDocument(IngestDocument other) {
        this(
            new IngestCtxMap(deepCopyMap(other.ctxMap.getSource()), other.ctxMap.getMetadata().clone()),
            deepCopyMap(other.ingestMetadata)
        );
    }

    /**
     * Constructor to create an IngestDocument from its constituent maps. The maps are shallow copied.
     */
    public IngestDocument(Map<String, Object> sourceAndMetadata, Map<String, Object> ingestMetadata) {
        Map<String, Object> source;
        Map<String, Object> metadata;
        if (sourceAndMetadata instanceof IngestCtxMap ingestCtxMap) {
            source = new HashMap<>(ingestCtxMap.getSource());
            metadata = new HashMap<>(ingestCtxMap.getMetadata().getMap());
        } else {
            metadata = Maps.newHashMapWithExpectedSize(Metadata.METADATA_NAMES.size());
            source = new HashMap<>(sourceAndMetadata);
            for (String key : Metadata.METADATA_NAMES) {
                if (sourceAndMetadata.containsKey(key)) {
                    metadata.put(key, source.remove(key));
                }
            }
        }
        this.ctxMap = new IngestCtxMap(source, new IngestDocMetadata(metadata, IngestCtxMap.getTimestamp(ingestMetadata)));
        this.ingestMetadata = new HashMap<>(ingestMetadata);
        this.templateModel = initializeTemplateModel();
    }

    /**
     * Constructor to create an IngestDocument from its constituent maps.
     */
    IngestDocument(IngestCtxMap ctxMap, Map<String, Object> ingestMetadata) {
        this.ctxMap = Objects.requireNonNull(ctxMap);
        this.ingestMetadata = Objects.requireNonNull(ingestMetadata);
        this.templateModel = initializeTemplateModel();
    }

    private DelegatingMapView initializeTemplateModel() {
        return new DelegatingMapView(ctxMap, Map.of(SOURCE_KEY, ctxMap, INGEST_KEY, ingestMetadata));
    }

    /**
     * Returns the value contained in the document for the provided path
     * @param path The path within the document in dot-notation
     * @param clazz The expected class of the field value
     * @return the value for the provided path if existing
     * @throws IllegalArgumentException if the path is null, empty, invalid, if the field doesn't exist
     * or if the field that is found at the provided path is not of the expected type.
     */
    public <T> T getFieldValue(String path, Class<T> clazz) {
        return getFieldValue(path, clazz, false);
    }

    /**
     * Returns the value contained in the document for the provided path
     *
     * @param path The path within the document in dot-notation
     * @param clazz The expected class of the field value
     * @param ignoreMissing The flag to determine whether to throw an exception when `path` is not found in the document.
     * @return the value for the provided path if existing, null otherwise.
     * @throws IllegalArgumentException only if ignoreMissing is false and the path is null, empty, invalid, if the field doesn't exist
     * or if the field that is found at the provided path is not of the expected type.
     */
    public <T> T getFieldValue(String path, Class<T> clazz, boolean ignoreMissing) {
        FailFastWriteField writeField = field(path);
        Object value;
        if (ignoreMissing) {
            value = writeField.get(null);
        } else {
            value = writeField.get();
        }
        return cast(path, value, clazz);
    }

    private FailFastWriteField field(String path) {
        if (Strings.isEmpty(path)) {
            throw new IllegalArgumentException("path cannot be null nor empty");
        }
        String newPath;
        Map<String, Object> initialContext;
        if (path.startsWith(INGEST_KEY_PREFIX)) {
            initialContext = ingestMetadata;
            newPath = path.substring(INGEST_KEY_PREFIX.length());
        } else {
            initialContext = ctxMap;
            if (path.startsWith(SOURCE_PREFIX)) {
                newPath = path.substring(SOURCE_PREFIX.length());
            } else {
                newPath = path;
            }
        }
        String[] pathElements = newPath.split("\\.");
        if (pathElements.length == 1 && pathElements[0].isEmpty()) {
            throw new IllegalArgumentException("path [" + path + "] is not valid");
        }

        return new FailFastWriteField(newPath, () -> initialContext);
    }

    /**
     * Returns the value contained in the document with the provided templated path
     * @param pathTemplate The path within the document in dot-notation
     * @param clazz The expected class of the field value
     * @return the value for the provided path if existing, null otherwise
     * @throws IllegalArgumentException if the pathTemplate is null, empty, invalid, if the field doesn't exist,
     * or if the field that is found at the provided path is not of the expected type.
     */
    public <T> T getFieldValue(TemplateScript.Factory pathTemplate, Class<T> clazz) {
        return getFieldValue(renderTemplate(pathTemplate), clazz);
    }

    /**
     * Returns the value contained in the document for the provided path as a byte array.
     * If the path value is a string, a base64 decode operation will happen.
     * If the path value is a byte array, it is just returned
     * @param path The path within the document in dot-notation
     * @return the byte array for the provided path if existing
     * @throws IllegalArgumentException if the path is null, empty, invalid, if the field doesn't exist
     * or if the field that is found at the provided path is not of the expected type.
     */
    public byte[] getFieldValueAsBytes(String path) {
        return getFieldValueAsBytes(path, false);
    }

    /**
     * Returns the value contained in the document for the provided path as a byte array.
     * If the path value is a string, a base64 decode operation will happen.
     * If the path value is a byte array, it is just returned
     * @param path The path within the document in dot-notation
     * @param ignoreMissing The flag to determine whether to throw an exception when `path` is not found in the document.
     * @return the byte array for the provided path if existing
     * @throws IllegalArgumentException if the path is null, empty, invalid, if the field doesn't exist
     * or if the field that is found at the provided path is not of the expected type.
     */
    public byte[] getFieldValueAsBytes(String path, boolean ignoreMissing) {
        Object object = getFieldValue(path, Object.class, ignoreMissing);
        if (object == null) {
            return null;
        } else if (object instanceof byte[] bytes) {
            return bytes;
        } else if (object instanceof String string) {
            return Base64.getDecoder().decode(string);
        } else {
            throw new IllegalArgumentException(
                "Content field [" + path + "] of unknown type [" + object.getClass().getName() + "], must be string or byte array"
            );
        }
    }

    /**
     * Checks whether the document contains a value for the provided templated path
     * @param fieldPathTemplate the template for the path within the document in dot-notation
     * @return true if the document contains a value for the field, false otherwise
     * @throws IllegalArgumentException if the path is null, empty or invalid
     */
    public boolean hasField(TemplateScript.Factory fieldPathTemplate) {
        return hasField(renderTemplate(fieldPathTemplate));
    }

    /**
     * Checks whether the document contains a value for the provided path
     * @param path The path within the document in dot-notation
     * @return true if the document contains a value for the field, false otherwise
     * @throws IllegalArgumentException if the path is null, empty or invalid.
     */
    public boolean hasField(String path) {
        return hasField(path, false);
    }

    /**
     * Checks whether the document contains a value for the provided path
     *
     * @param path           The path within the document in dot-notation
     * @param failOutOfRange Whether to throw an IllegalArgumentException if array is accessed outside of its range
     * @return true if the document contains a value for the field, false otherwise
     * @throws IllegalArgumentException if the path is null, empty or invalid.
     */
    public boolean hasField(String path, boolean failOutOfRange) {
        return field(path).exists(failOutOfRange);
    }

    /**
     * Removes the field identified by the provided path.
     * @param fieldPathTemplate Resolves to the path with dot-notation within the document
     * @throws IllegalArgumentException if the path is null, empty, invalid or if the field doesn't exist.
     */
    public void removeField(TemplateScript.Factory fieldPathTemplate) {
        removeField(renderTemplate(fieldPathTemplate));
    }

    /**
     * Removes the field identified by the provided path.
     * @param path the path of the field to be removed
     * @throws IllegalArgumentException if the path is null, empty, invalid or if the field doesn't exist.
     */
    public void removeField(String path) {
        field(path).remove();
    }

    /**
     * Appends the provided value to the provided path in the document.
     * Any non existing path element will be created.
     * If the path identifies a list, the value will be appended to the existing list.
     * If the path identifies a scalar, the scalar will be converted to a list and
     * the provided value will be added to the newly created list.
     * Supports multiple values too provided in forms of list, in that case all the values will be appended to the
     * existing (or newly created) list.
     * @param path The path within the document in dot-notation
     * @param value The value or values to append to the existing ones
     * @throws IllegalArgumentException if the path is null, empty or invalid.
     */
    public void appendFieldValue(String path, Object value) {
        appendFieldValue(path, value, true);
    }

    /**
     * Appends the provided value to the provided path in the document.
     * Any non existing path element will be created.
     * If the path identifies a list, the value will be appended to the existing list.
     * If the path identifies a scalar, the scalar will be converted to a list and
     * the provided value will be added to the newly created list.
     * Supports multiple values too provided in forms of list, in that case all the values will be appended to the
     * existing (or newly created) list.
     * @param path The path within the document in dot-notation
     * @param value The value or values to append to the existing ones
     * @param allowDuplicates When false, any values that already exist in the field will not be added
     * @throws IllegalArgumentException if the path is null, empty or invalid.
     */
    public void appendFieldValue(String path, Object value, boolean allowDuplicates) {
        setFieldValue(path, value, true, allowDuplicates);
    }

    /**
     * Appends the provided value to the provided path in the document.
     * Any non existing path element will be created.
     * If the path identifies a list, the value will be appended to the existing list.
     * If the path identifies a scalar, the scalar will be converted to a list and
     * the provided value will be added to the newly created list.
     * Supports multiple values too provided in forms of list, in that case all the values will be appended to the
     * existing (or newly created) list.
     * @param fieldPathTemplate Resolves to the path with dot-notation within the document
     * @param valueSource The value source that will produce the value or values to append to the existing ones
     * @param allowDuplicates When false, any values that already exist in the field will not be added
     * @throws IllegalArgumentException if the path is null, empty or invalid.
     */
    public void appendFieldValue(TemplateScript.Factory fieldPathTemplate, ValueSource valueSource, boolean allowDuplicates) {
        appendFieldValue(
            fieldPathTemplate.newInstance(templateModel).execute(),
            valueSource.copyAndResolve(templateModel),
            allowDuplicates
        );
    }

    /**
     * Sets the provided value to the provided path in the document.
     * Any non existing path element will be created.
     * If the last item in the path is a list, the value will replace the existing list as a whole.
     * Use {@link #appendFieldValue(String, Object)} to append values to lists instead.
     * @param path The path within the document in dot-notation
     * @param value The value to put in for the path key
     * @throws IllegalArgumentException if the path is null, empty, invalid or if the value cannot be set to the
     * item identified by the provided path.
     */
    public void setFieldValue(String path, Object value) {
        setFieldValue(path, value, false, true);
    }

    /**
     * Sets the provided value to the provided path in the document.
     * Any non existing path element will be created. If the last element is a list,
     * the value will replace the existing list.
     * @param fieldPathTemplate Resolves to the path with dot-notation within the document
     * @param valueSource The value source that will produce the value to put in for the path key
     * @throws IllegalArgumentException if the path is null, empty, invalid or if the value cannot be set to the
     * item identified by the provided path.
     */
    public void setFieldValue(TemplateScript.Factory fieldPathTemplate, ValueSource valueSource) {
        setFieldValue(fieldPathTemplate.newInstance(templateModel).execute(), valueSource.copyAndResolve(templateModel));
    }

    /**
     * Sets the provided value to the provided path in the document.
     * Any non existing path element will be created. If the last element is a list,
     * the value will replace the existing list.
     * @param fieldPathTemplate Resolves to the path with dot-notation within the document
     * @param valueSource The value source that will produce the value to put in for the path key
     * @param ignoreEmptyValue The flag to determine whether to exit quietly when the value produced by TemplatedValue is null or empty
     * @throws IllegalArgumentException if the path is null, empty, invalid or if the value cannot be set to the
     * item identified by the provided path.
     */
    public void setFieldValue(TemplateScript.Factory fieldPathTemplate, ValueSource valueSource, boolean ignoreEmptyValue) {
        Object value = valueSource.copyAndResolve(templateModel);
        if (ignoreEmptyValue && valueSource instanceof ValueSource.TemplatedValue) {
            if (value == null) {
                return;
            }
            String valueStr = (String) value;
            if (valueStr.isEmpty()) {
                return;
            }
        }

        setFieldValue(fieldPathTemplate.newInstance(templateModel).execute(), value);
    }

    /**
     * Sets the provided value to the provided path in the document.
     * Any non existing path element will be created. If the last element is a list,
     * the value will replace the existing list.
     * @param fieldPathTemplate Resolves to the path with dot-notation within the document
     * @param value The value to put in for the path key
     * @param ignoreEmptyValue The flag to determine whether to exit quietly when the value produced by TemplatedValue is null or empty
     * @throws IllegalArgumentException if the path is null, empty, invalid or if the value cannot be set to the
     * item identified by the provided path.
     */
    public void setFieldValue(TemplateScript.Factory fieldPathTemplate, Object value, boolean ignoreEmptyValue) {
        if (ignoreEmptyValue) {
            if (value == null) {
                return;
            }
            if (value instanceof String string) {
                if (string.isEmpty()) {
                    return;
                }
            }
        }

        setFieldValue(fieldPathTemplate.newInstance(templateModel).execute(), value);
    }

    private void setFieldValue(String path, Object value, boolean append, boolean allowDuplicates) {
        FailFastWriteField writeField = field(path);
        if (append) {
            writeField.append(value, allowDuplicates);
        } else {
            writeField.set(value);
        }
    }

    private static <T> T cast(String path, Object object, Class<T> clazz) {
        if (object == null) {
            return null;
        }
        if (clazz.isInstance(object)) {
            return clazz.cast(object);
        }
        throw new IllegalArgumentException(
            "field [" + path + "] of type [" + object.getClass().getName() + "] cannot be cast to [" + clazz.getName() + "]"
        );
    }

    public String renderTemplate(TemplateScript.Factory template) {
        return template.newInstance(templateModel).execute();
    }

    /**
     * Get source and metadata map
     */
    public Map<String, Object> getSourceAndMetadata() {
        return ctxMap;
    }

    /**
     * Get the CtxMap
     */
    public CtxMap<?> getCtxMap() {
        return ctxMap;
    }

    /**
     * Get the strongly typed metadata
     */
    public org.elasticsearch.script.Metadata getMetadata() {
        return ctxMap.getMetadata();
    }

    /**
     * Get all source values in a Map
     */
    public Map<String, Object> getSource() {
        return ctxMap.getSource();
    }

    /**
     * Returns the available ingest metadata fields, by default only timestamp, but it is possible to set additional ones.
     * Use only for reading values, modify them instead using {@link #setFieldValue(String, Object)} and {@link #removeField(String)}
     */
    public Map<String, Object> getIngestMetadata() {
        return this.ingestMetadata;
    }

    @SuppressWarnings("unchecked")
    public static <K, V> Map<K, V> deepCopyMap(Map<K, V> source) {
        return (Map<K, V>) deepCopy(source);
    }

    public static Object deepCopy(Object value) {
        if (value instanceof Map<?, ?> mapValue) {
            Map<Object, Object> copy = Maps.newMapWithExpectedSize(mapValue.size());
            for (Map.Entry<?, ?> entry : mapValue.entrySet()) {
                copy.put(entry.getKey(), deepCopy(entry.getValue()));
            }
            // TODO(stu): should this check for IngestCtxMap in addition to Map?
            return copy;
        } else if (value instanceof List<?> listValue) {
            List<Object> copy = new ArrayList<>(listValue.size());
            for (Object itemValue : listValue) {
                copy.add(deepCopy(itemValue));
            }
            return copy;
        } else if (value instanceof Set<?> setValue) {
            Set<Object> copy = Sets.newHashSetWithExpectedSize(setValue.size());
            for (Object itemValue : setValue) {
                copy.add(deepCopy(itemValue));
            }
            return copy;
        } else if (value instanceof byte[] bytes) {
            return Arrays.copyOf(bytes, bytes.length);
        } else if (value instanceof double[][] doubles) {
            double[][] result = new double[doubles.length][];
            for (int i = 0; i < doubles.length; i++) {
                result[i] = Arrays.copyOf(doubles[i], doubles[i].length);
            }
            return result;
        } else if (value instanceof double[] doubles) {
            return Arrays.copyOf(doubles, doubles.length);
        } else if (value == null
            || value instanceof String
            || value instanceof Integer
            || value instanceof Long
            || value instanceof Float
            || value instanceof Double
            || value instanceof Boolean
            || value instanceof ZonedDateTime) {
                return value;
            } else if (value instanceof Date date) {
                return date.clone();
            } else {
                throw new IllegalArgumentException("unexpected value type [" + value.getClass() + "]");
            }
    }

    public static Set<String> getAllFields(Map<String, Object> input) {
        return getAllFields(input, "");
    }

    @SuppressWarnings("unchecked")
    private static Set<String> getAllFields(Map<String, Object> input, String prefix) {
        Set<String> allFields = Sets.newHashSet();

        input.forEach((k, v) -> {
            allFields.add(prefix + k);

            if (v instanceof Map<?, ?> mapValue) {
                allFields.addAll(getAllFields((Map<String, Object>) mapValue, prefix + k + "."));
            }
        });

        return allFields;
    }

    /**
     * Executes the given pipeline with for this document unless the pipeline has already been executed
     * for this document.
     *
     * @param pipeline the pipeline to execute
     * @param handler handles the result or failure
     */
    public void executePipeline(Pipeline pipeline, BiConsumer<IngestDocument, Exception> handler) {
        // shortcut if the pipeline is empty
        if (pipeline.getProcessors().isEmpty()) {
            handler.accept(this, null);
            return;
        }

        if (executedPipelines.add(pipeline.getId())) {
            Object previousPipeline = ingestMetadata.put("pipeline", pipeline.getId());
            pipeline.execute(this, (result, e) -> {
                executedPipelines.remove(pipeline.getId());
                if (previousPipeline != null) {
                    ingestMetadata.put("pipeline", previousPipeline);
                } else {
                    ingestMetadata.remove("pipeline");
                }
                handler.accept(result, e);
            });
        } else {
            handler.accept(null, new IllegalStateException(PIPELINE_CYCLE_ERROR_MESSAGE + pipeline.getId()));
        }
    }

    /**
     * @return a pipeline stack; all pipelines that are in execution by this document in reverse order
     */
    List<String> getPipelineStack() {
        List<String> pipelineStack = new ArrayList<>(executedPipelines);
        Collections.reverse(pipelineStack);
        return pipelineStack;
    }

    /**
     * Adds an index to the index history for this document, returning true if the index
     * was added to the index history (i.e. if it wasn't already in the index history).
     *
     * @param index the index to potentially add to the index history
     * @return true if the index history did not already contain the index in question
     */
    public boolean updateIndexHistory(String index) {
        return indexHistory.add(index);
    }

    /**
     * @return an unmodifiable view of the document's index history
     */
    public Set<String> getIndexHistory() {
        return Collections.unmodifiableSet(indexHistory);
    }

    /**
     * @return Whether a self referencing check should be performed
     */
    public boolean doNoSelfReferencesCheck() {
        return doNoSelfReferencesCheck;
    }

    /**
     * Whether the ingest framework should perform a self referencing check after this ingest document
     * has been processed by all pipelines. Doing this check adds an extra tax to ingest and should
     * only be performed when really needed. Only if a processor is executed that could add self referencing
     * maps or lists then this check must be performed. Most processors will not be able to do this, hence
     * the default is <code>false</code>.
     *
     * @param doNoSelfReferencesCheck Whether a self referencing check should be performed
     */
    public void doNoSelfReferencesCheck(boolean doNoSelfReferencesCheck) {
        this.doNoSelfReferencesCheck = doNoSelfReferencesCheck;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        IngestDocument other = (IngestDocument) obj;
        return Objects.equals(ctxMap, other.ctxMap) && Maps.deepEquals(ingestMetadata, other.ingestMetadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ctxMap, ingestMetadata);
    }

    @Override
    public String toString() {
        return "IngestDocument{" + " sourceAndMetadata=" + ctxMap + ", ingestMetadata=" + ingestMetadata + '}';
    }

    public void reroute(String destIndex) {
        getMetadata().setIndex(destIndex);
        reroute = true;
    }

    /**
     * The document is redirected to another target.
     * This implies that we'll skip the current pipeline and invoke the default pipeline of the new target
     *
     * @return whether the document is redirected to another target
     */
    boolean isReroute() {
        return reroute;
    }

    /**
     * Set the {@link #reroute} flag to false so that subsequent calls to {@link #isReroute()} will return false until/unless
     * {@link #reroute(String)} is called.
     */
    void resetReroute() {
        reroute = false;
    }

    public enum Metadata {
        INDEX(IndexFieldMapper.NAME),
        TYPE("_type"),
        ID(IdFieldMapper.NAME),
        ROUTING(RoutingFieldMapper.NAME),
        VERSION(VersionFieldMapper.NAME),
        VERSION_TYPE("_version_type"),
        IF_SEQ_NO("_if_seq_no"),
        IF_PRIMARY_TERM("_if_primary_term"),
        DYNAMIC_TEMPLATES("_dynamic_templates");

        private static final Set<String> METADATA_NAMES = Arrays.stream(Metadata.values())
            .map(metadata -> metadata.fieldName)
            .collect(Collectors.toSet());

        private final String fieldName;

        Metadata(String fieldName) {
            this.fieldName = fieldName;
        }

        public static boolean isMetadata(String field) {
            return METADATA_NAMES.contains(field);
        }

        public String getFieldName() {
            return fieldName;
        }
    }

    /**
     * Provides a shallowly read-only, very limited, map-like view of two maps. The only methods that are implemented are
     * {@link Map#get(Object)} and {@link Map#containsKey(Object)}, everything else throws UnsupportedOperationException.
     * <p>
     * The overrides map has higher priority than the primary map -- values in that map under some key will take priority over values
     * in the primary map under the same key.
     *
     * @param primary the primary map
     * @param overrides the overrides map
     */
    private record DelegatingMapView(Map<String, Object> primary, Map<String, Object> overrides) implements Map<String, Object> {

        @Override
        public boolean containsKey(Object key) {
            // most normal uses of this in practice will end up passing in keys that match the primary, rather than the overrides,
            // in which case we can shortcut by checking the primary first
            return primary.containsKey(key) || overrides.containsKey(key);
        }

        @Override
        public Object get(Object key) {
            // null values in the overrides map are treated as *key not present*, so we don't have to do a containsKey check here --
            // if the overrides map returns null we can simply delegate to the primary
            Object result = overrides.get(key);
            return result != null ? result : primary.get(key);
        }

        @Override
        public int size() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isEmpty() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean containsValue(Object value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object put(String key, Object value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object remove(Object key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putAll(Map<? extends String, ?> m) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<String> keySet() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Collection<Object> values() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<Entry<String, Object>> entrySet() {
            throw new UnsupportedOperationException();
        }
    }
}
