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

package org.elasticsearch.action.index;

import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.support.replication.ReplicatedWriteRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Index request to index a typed JSON document into a specific index and make it searchable. Best
 * created using {@link org.elasticsearch.client.Requests#indexRequest(String)}.
 *
 * The index requires the {@link #index()}, {@link #type(String)}, {@link #id(String)} and
 * {@link #source(byte[], XContentType)} to be set.
 *
 * The source (content to index) can be set in its bytes form using ({@link #source(byte[], XContentType)}),
 * its string form ({@link #source(String, XContentType)}) or using a {@link org.elasticsearch.common.xcontent.XContentBuilder}
 * ({@link #source(org.elasticsearch.common.xcontent.XContentBuilder)}).
 *
 * If the {@link #id(String)} is not set, it will be automatically generated.
 *
 * @see IndexResponse
 * @see org.elasticsearch.client.Requests#indexRequest(String)
 * @see org.elasticsearch.client.Client#index(IndexRequest)
 */
public class IndexRequest extends ReplicatedWriteRequest<IndexRequest> implements DocWriteRequest<IndexRequest>, CompositeIndicesRequest {

    private String type;
    private String id;
    @Nullable
    private String routing;
    @Nullable
    private String parent;

    private BytesReference source;

    private OpType opType = OpType.INDEX;

    private long version = Versions.MATCH_ANY;
    private VersionType versionType = VersionType.INTERNAL;

    private XContentType contentType;

    private String pipeline;

    /**
     * Value for {@link #getAutoGeneratedTimestamp()} if the document has an external
     * provided ID.
     */
    public static final int UNSET_AUTO_GENERATED_TIMESTAMP = -1;

    private long autoGeneratedTimestamp = UNSET_AUTO_GENERATED_TIMESTAMP;

    private boolean isRetry = false;


    public IndexRequest() {
    }

    /**
     * Constructs a new index request against the specific index. The {@link #type(String)}
     * {@link #source(byte[], XContentType)} must be set.
     */
    public IndexRequest(String index) {
        this.index = index;
    }

    /**
     * Constructs a new index request against the specific index and type. The
     * {@link #source(byte[])} must be set.
     */
    public IndexRequest(String index, String type) {
        this.index = index;
        this.type = type;
    }

    /**
     * Constructs a new index request against the index, type, id and using the source.
     *
     * @param index The index to index into
     * @param type  The type to index into
     * @param id    The id of document
     */
    public IndexRequest(String index, String type, String id) {
        this.index = index;
        this.type = type;
        this.id = id;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (type == null) {
            validationException = addValidationError("type is missing", validationException);
        }
        if (source == null) {
            validationException = addValidationError("source is missing", validationException);
        }
        if (contentType == null) {
            validationException = addValidationError("content type is missing", validationException);
        }
        final long resolvedVersion = resolveVersionDefaults();
        if (opType() == OpType.CREATE) {
            if (versionType != VersionType.INTERNAL) {
                validationException = addValidationError("create operations only support internal versioning. use index instead", validationException);
                return validationException;
            }

            if (resolvedVersion != Versions.MATCH_DELETED) {
                validationException = addValidationError("create operations do not support explicit versions. use index instead", validationException);
                return validationException;
            }
        }

        if (opType() != OpType.INDEX && id == null) {
            addValidationError("an id is required for a " + opType() + " operation", validationException);
        }

        if (!versionType.validateVersionForWrites(resolvedVersion)) {
            validationException = addValidationError("illegal version value [" + resolvedVersion + "] for version type [" + versionType.name() + "]", validationException);
        }

        if (versionType == VersionType.FORCE) {
            validationException = addValidationError("version type [force] may no longer be used", validationException);
        }

        if (id != null && id.getBytes(StandardCharsets.UTF_8).length > 512) {
            validationException = addValidationError("id is too long, must be no longer than 512 bytes but was: " +
                            id.getBytes(StandardCharsets.UTF_8).length, validationException);
        }

        if (id == null && (versionType == VersionType.INTERNAL && resolvedVersion == Versions.MATCH_ANY) == false) {
            validationException = addValidationError("an id must be provided if version type or value are set", validationException);
        }

        return validationException;
    }

    /**
     * The content type. This will be used when generating a document from user provided objects like Maps and when parsing the
     * source at index time
     */
    public XContentType getContentType() {
        return contentType;
    }

    /**
     * The type of the indexed document.
     */
    @Override
    public String type() {
        return type;
    }

    /**
     * Sets the type of the indexed document.
     */
    public IndexRequest type(String type) {
        this.type = type;
        return this;
    }

    /**
     * The id of the indexed document. If not set, will be automatically generated.
     */
    @Override
    public String id() {
        return id;
    }

    /**
     * Sets the id of the indexed document. If not set, will be automatically generated.
     */
    public IndexRequest id(String id) {
        this.id = id;
        return this;
    }

    /**
     * Controls the shard routing of the request. Using this value to hash the shard
     * and not the id.
     */
    @Override
    public IndexRequest routing(String routing) {
        if (routing != null && routing.length() == 0) {
            this.routing = null;
        } else {
            this.routing = routing;
        }
        return this;
    }

    /**
     * Controls the shard routing of the request. Using this value to hash the shard
     * and not the id.
     */
    @Override
    public String routing() {
        return this.routing;
    }

    /**
     * Sets the parent id of this document.
     */
    public IndexRequest parent(String parent) {
        this.parent = parent;
        return this;
    }

    @Override
    public String parent() {
        return this.parent;
    }

    /**
     * Sets the ingest pipeline to be executed before indexing the document
     */
    public IndexRequest setPipeline(String pipeline) {
        this.pipeline = pipeline;
        return this;
    }

    /**
     * Returns the ingest pipeline to be executed before indexing the document
     */
    public String getPipeline() {
        return this.pipeline;
    }

    /**
     * The source of the document to index, recopied to a new array if it is unsafe.
     */
    public BytesReference source() {
        return source;
    }

    public Map<String, Object> sourceAsMap() {
        return XContentHelper.convertToMap(source, false, contentType).v2();
    }

    /**
     * Index the Map in {@link Requests#INDEX_CONTENT_TYPE} format
     *
     * @param source The map to index
     */
    public IndexRequest source(Map source) throws ElasticsearchGenerationException {
        return source(source, Requests.INDEX_CONTENT_TYPE);
    }

    /**
     * Index the Map as the provided content type.
     *
     * @param source The map to index
     */
    public IndexRequest source(Map source, XContentType contentType) throws ElasticsearchGenerationException {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(contentType);
            builder.map(source);
            return source(builder);
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate [" + source + "]", e);
        }
    }

    /**
     * Sets the document source to index.
     *
     * @deprecated use {@link #source(String, XContentType)}
     */
    @Deprecated
    public IndexRequest source(String source) {
        return source(new BytesArray(source), XContentFactory.xContentType(source));
    }

    /**
     * Sets the document source to index.
     *
     * Note, its preferable to either set it using {@link #source(org.elasticsearch.common.xcontent.XContentBuilder)}
     * or using the {@link #source(byte[], XContentType)}.
     */
    public IndexRequest source(String source, XContentType xContentType) {
        return source(new BytesArray(source), xContentType);
    }

    /**
     * Sets the content source to index.
     */
    public IndexRequest source(XContentBuilder sourceBuilder) {
        return source(sourceBuilder.bytes(), sourceBuilder.contentType());
    }

    /**
     * Sets the content source to index using the default content type ({@link Requests#INDEX_CONTENT_TYPE})
     * <p>
     * <b>Note: the number of objects passed to this method must be an even
     * number. Also the first argument in each pair (the field name) must have a
     * valid String representation.</b>
     * </p>
     */
    public IndexRequest source(Object... source) {
        return source(Requests.INDEX_CONTENT_TYPE, source);
    }

    /**
     * Sets the content source to index.
     * <p>
     * <b>Note: the number of objects passed to this method as varargs must be an even
     * number. Also the first argument in each pair (the field name) must have a
     * valid String representation.</b>
     * </p>
     */
    public IndexRequest source(XContentType xContentType, Object... source) {
        if (source.length % 2 != 0) {
            throw new IllegalArgumentException("The number of object passed must be even but was [" + source.length + "]");
        }
        if (source.length == 2 && source[0] instanceof BytesReference && source[1] instanceof Boolean) {
            throw new IllegalArgumentException("you are using the removed method for source with bytes and unsafe flag, the unsafe flag was removed, please just use source(BytesReference)");
        }
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(xContentType);
            builder.startObject();
            for (int i = 0; i < source.length; i++) {
                builder.field(source[i++].toString(), source[i]);
            }
            builder.endObject();
            return source(builder);
        } catch (IOException e) {
            throw new ElasticsearchGenerationException("Failed to generate", e);
        }
    }

    /**
     * Sets the document to index in bytes form.
     * @deprecated use {@link #source(BytesReference, XContentType)}
     */
    @Deprecated
    public IndexRequest source(BytesReference source) {
        return source(source, XContentFactory.xContentType(source));

    }

    /**
     * Sets the document to index in bytes form.
     */
    public IndexRequest source(BytesReference source, XContentType xContentType) {
        this.source = Objects.requireNonNull(source);
        this.contentType = Objects.requireNonNull(xContentType);
        return this;
    }

    /**
     * Sets the document to index in bytes form.
     * @deprecated use {@link #source(byte[], XContentType)}
     */
    @Deprecated
    public IndexRequest source(byte[] source) {
        return source(source, 0, source.length);
    }

    /**
     * Sets the document to index in bytes form.
     */
    public IndexRequest source(byte[] source, XContentType xContentType) {
        return source(source, 0, source.length, xContentType);
    }

    /**
     * Sets the document to index in bytes form (assumed to be safe to be used from different
     * threads).
     *
     * @param source The source to index
     * @param offset The offset in the byte array
     * @param length The length of the data
     * @deprecated use {@link #source(byte[], int, int, XContentType)}
     */
    @Deprecated
    public IndexRequest source(byte[] source, int offset, int length) {
        return source(new BytesArray(source, offset, length), XContentFactory.xContentType(source));
    }

    /**
     * Sets the document to index in bytes form (assumed to be safe to be used from different
     * threads).
     *
     * @param source The source to index
     * @param offset The offset in the byte array
     * @param length The length of the data
     */
    public IndexRequest source(byte[] source, int offset, int length, XContentType xContentType) {
        return source(new BytesArray(source, offset, length), xContentType);
    }

    /**
     * Sets the type of operation to perform.
     */
    public IndexRequest opType(OpType opType) {
        if (opType != OpType.CREATE && opType != OpType.INDEX) {
            throw new IllegalArgumentException("opType must be 'create' or 'index', found: [" + opType + "]");
        }
        this.opType = opType;
        return this;
    }

    /**
     * Sets a string representation of the {@link #opType(OpType)}. Can
     * be either "index" or "create".
     */
    public IndexRequest opType(String opType) {
        String op = opType.toLowerCase(Locale.ROOT);
        if (op.equals("create")) {
            opType(OpType.CREATE);
        } else if (op.equals("index")) {
            opType(OpType.INDEX);
        } else {
            throw new IllegalArgumentException("opType must be 'create' or 'index', found: [" + opType + "]");
        }
        return this;
    }


    /**
     * Set to <tt>true</tt> to force this index to use {@link OpType#CREATE}.
     */
    public IndexRequest create(boolean create) {
        if (create) {
            return opType(OpType.CREATE);
        } else {
            return opType(OpType.INDEX);
        }
    }

    @Override
    public OpType opType() {
        return this.opType;
    }

    @Override
    public IndexRequest version(long version) {
        this.version = version;
        return this;
    }

    /**
     * Returns stored version. If currently stored version is {@link Versions#MATCH_ANY} and
     * opType is {@link OpType#CREATE}, returns {@link Versions#MATCH_DELETED}.
     */
    @Override
    public long version() {
        return resolveVersionDefaults();
    }

    /**
     * Resolves the version based on operation type {@link #opType()}.
     */
    private long resolveVersionDefaults() {
        if (opType == OpType.CREATE && version == Versions.MATCH_ANY) {
            return Versions.MATCH_DELETED;
        } else {
            return version;
        }
    }

    @Override
    public IndexRequest versionType(VersionType versionType) {
        this.versionType = versionType;
        return this;
    }

    @Override
    public VersionType versionType() {
        return this.versionType;
    }


    public void process(@Nullable MappingMetaData mappingMd, boolean allowIdGeneration, String concreteIndex) {
        if (mappingMd != null) {
            // might as well check for routing here
            if (mappingMd.routing().required() && routing == null) {
                throw new RoutingMissingException(concreteIndex, type, id);
            }

            if (parent != null && !mappingMd.hasParentField()) {
                throw new IllegalArgumentException("Can't specify parent if no parent field has been configured");
            }
        } else {
            if (parent != null) {
                throw new IllegalArgumentException("Can't specify parent if no parent field has been configured");
            }
        }

        // generate id if not already provided and id generation is allowed
        if (allowIdGeneration && id == null) {
            assert autoGeneratedTimestamp == -1;
            autoGeneratedTimestamp = Math.max(0, System.currentTimeMillis()); // extra paranoia
            id(UUIDs.base64UUID());
        }
    }

    /* resolve the routing if needed */
    public void resolveRouting(MetaData metaData) {
        routing(metaData.resolveIndexRouting(parent, routing, index));
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        type = in.readOptionalString();
        id = in.readOptionalString();
        routing = in.readOptionalString();
        parent = in.readOptionalString();
        if (in.getVersion().before(Version.V_6_0_0_alpha1_UNRELEASED)) {
            in.readOptionalString(); // timestamp
            in.readOptionalWriteable(TimeValue::new); // ttl
        }
        source = in.readBytesReference();
        opType = OpType.fromId(in.readByte());
        version = in.readLong();
        versionType = VersionType.fromValue(in.readByte());
        pipeline = in.readOptionalString();
        isRetry = in.readBoolean();
        autoGeneratedTimestamp = in.readLong();
        if (in.getVersion().after(Version.V_5_3_0_UNRELEASED)) { // TODO update to onOrAfter after backporting
            contentType = in.readOptionalWriteable(XContentType::readFrom);
        } else {
            contentType = XContentFactory.xContentType(source);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(type);
        out.writeOptionalString(id);
        out.writeOptionalString(routing);
        out.writeOptionalString(parent);
        if (out.getVersion().before(Version.V_6_0_0_alpha1_UNRELEASED)) {
            // Serialize a fake timestamp. 5.x expect this value to be set by the #process method so we can't use null.
            // On the other hand, indices created on 5.x do not index the timestamp field.  Therefore passing a 0 (or any value) for
            // the transport layer OK as it will be ignored.
            out.writeOptionalString("0");
            out.writeOptionalWriteable(null);
        }
        out.writeBytesReference(source);
        out.writeByte(opType.getId());
        // ES versions below 5.1.2 don't know about resolveVersionDefaults but resolve the version eagerly (which messes with validation).
        if (out.getVersion().before(Version.V_5_1_2_UNRELEASED)) {
            out.writeLong(resolveVersionDefaults());
        } else {
            out.writeLong(version);
        }
        out.writeByte(versionType.getValue());
        out.writeOptionalString(pipeline);
        out.writeBoolean(isRetry);
        out.writeLong(autoGeneratedTimestamp);
        if (out.getVersion().after(Version.V_5_3_0_UNRELEASED)) { // TODO update to onOrAfter after backporting
            out.writeOptionalWriteable(contentType);
        }
    }

    @Override
    public String toString() {
        String sSource = "_na_";
        try {
            sSource = XContentHelper.convertToJson(source, false);
        } catch (Exception e) {
            // ignore
        }
        return "index {[" + index + "][" + type + "][" + id + "], source[" + sSource + "]}";
    }


    /**
     * Returns <code>true</code> if this request has been sent to a shard copy more than once.
     */
    public boolean isRetry() {
        return isRetry;
    }

    @Override
    public void onRetry() {
        isRetry = true;
    }

    /**
     * Returns the timestamp the auto generated ID was created or {@value #UNSET_AUTO_GENERATED_TIMESTAMP} if the
     * document has no auto generated timestamp. This method will return a positive value iff the id was auto generated.
     */
    public long getAutoGeneratedTimestamp() {
        return autoGeneratedTimestamp;
    }
}
