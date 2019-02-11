/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.extractor;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesAction;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.datafeed.extractor.fields.ExtractedField;
import org.elasticsearch.xpack.ml.datafeed.extractor.fields.ExtractedFields;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataFrameDataExtractorFactory {

    /**
     * Fields to ignore. These are mostly internal meta fields.
     */
    private static final List<String> IGNORE_FIELDS = Arrays.asList("_id", "_field_names", "_index", "_parent", "_routing", "_seq_no",
        "_source", "_type", "_uid", "_version", "_feature", "_ignored");

    /**
     * The types supported by data frames
     */
    private static final Set<String> COMPATIBLE_FIELD_TYPES;

    static {
        Set<String> compatibleTypes = Stream.of(NumberFieldMapper.NumberType.values())
            .map(NumberFieldMapper.NumberType::typeName)
            .collect(Collectors.toSet());
        compatibleTypes.add("scaled_float"); // have to add manually since scaled_float is in a module

        COMPATIBLE_FIELD_TYPES = Collections.unmodifiableSet(compatibleTypes);
    }

    private final Client client;
    private final String analyticsId;
    private final String index;
    private final ExtractedFields extractedFields;

    private DataFrameDataExtractorFactory(Client client, String analyticsId, String index, ExtractedFields extractedFields) {
        this.client = Objects.requireNonNull(client);
        this.analyticsId = Objects.requireNonNull(analyticsId);
        this.index = Objects.requireNonNull(index);
        this.extractedFields = Objects.requireNonNull(extractedFields);
    }

    public DataFrameDataExtractor newExtractor(boolean includeSource) {
        DataFrameDataExtractorContext context = new DataFrameDataExtractorContext(
                analyticsId,
                extractedFields,
                Arrays.asList(index),
                QueryBuilders.matchAllQuery(),
                1000,
                Collections.emptyMap(),
                includeSource
            );
        return new DataFrameDataExtractor(client, context);
    }

    /**
     * Validate and create a new extractor factory
     *
     * The destination index must exist and contain at least 1 compatible field or validations will fail.
     *
     * @param client ES Client used to make calls against the cluster
     * @param headers Headers to use
     * @param config The config from which to create the extractor factory
     * @param listener The listener to notify on creation or failure
     */
    public static void create(Client client,
                              Map<String, String> headers,
                              DataFrameAnalyticsConfig config,
                              ActionListener<DataFrameDataExtractorFactory> listener) {

        validateIndexAndExtractFields(client, headers, config.getDest(), ActionListener.wrap(
            extractedFields -> listener.onResponse(
                new DataFrameDataExtractorFactory(client, config.getId(), config.getDest(), extractedFields)),
            listener::onFailure
        ));
    }

    /**
     * Validates the source index and analytics config
     *
     * @param client ES Client to make calls
     * @param headers Headers for auth
     * @param config Analytics config to validate
     * @param listener The listener to notify on failure or completion
     */
    public static void validateConfigAndSourceIndex(Client client,
                                                    Map<String, String> headers,
                                                    DataFrameAnalyticsConfig config,
                                                    ActionListener<Boolean> listener) {
        validateIndexAndExtractFields(client, headers, config.getSource(), ActionListener.wrap(
            fields -> {
                config.getParsedQuery(); // validate query is acceptable
                listener.onResponse(true);
            },
            listener::onFailure
        ));
    }

    // Visible for testing
    static ExtractedFields detectExtractedFields(String index, FieldCapabilitiesResponse fieldCapabilitiesResponse) {
        Set<String> fields = fieldCapabilitiesResponse.get().keySet();
        fields.removeAll(IGNORE_FIELDS);
        removeFieldsWithIncompatibleTypes(fields, fieldCapabilitiesResponse);
        List<String> sortedFields = new ArrayList<>(fields);
        // We sort the fields to ensure the checksum for each document is deterministic
        Collections.sort(sortedFields);
        ExtractedFields extractedFields = ExtractedFields.build(sortedFields, Collections.emptySet(), fieldCapabilitiesResponse)
                .filterFields(ExtractedField.ExtractionMethod.DOC_VALUE);
        if (extractedFields.getAllFields().isEmpty()) {
            throw ExceptionsHelper.badRequestException("No compatible fields could be detected in index [{}]", index);
        }
        return extractedFields;
    }

    private static void removeFieldsWithIncompatibleTypes(Set<String> fields, FieldCapabilitiesResponse fieldCapabilitiesResponse) {
        Iterator<String> fieldsIterator = fields.iterator();
        while (fieldsIterator.hasNext()) {
            String field = fieldsIterator.next();
            Map<String, FieldCapabilities> fieldCaps = fieldCapabilitiesResponse.getField(field);
            if (fieldCaps == null || COMPATIBLE_FIELD_TYPES.containsAll(fieldCaps.keySet()) == false) {
                fieldsIterator.remove();
            }
        }
    }

    private static void validateIndexAndExtractFields(Client client,
                                                      Map<String, String> headers,
                                                      String index,
                                                      ActionListener<ExtractedFields> listener) {
        // Step 2. Extract fields (if possible) and notify listener
        ActionListener<FieldCapabilitiesResponse> fieldCapabilitiesHandler = ActionListener.wrap(
            fieldCapabilitiesResponse -> listener.onResponse(detectExtractedFields(index, fieldCapabilitiesResponse)),
            e -> {
                if (e instanceof IndexNotFoundException) {
                    listener.onFailure(new ResourceNotFoundException("cannot retrieve data because index "
                        + ((IndexNotFoundException) e).getIndex() + " does not exist"));
                } else {
                    listener.onFailure(e);
                }
            }
        );

        // Step 1. Get field capabilities necessary to build the information of how to extract fields
        FieldCapabilitiesRequest fieldCapabilitiesRequest = new FieldCapabilitiesRequest();
        fieldCapabilitiesRequest.indices(index);
        fieldCapabilitiesRequest.fields("*");
        ClientHelper.executeWithHeaders(headers, ClientHelper.ML_ORIGIN, client, () -> {
            client.execute(FieldCapabilitiesAction.INSTANCE, fieldCapabilitiesRequest, fieldCapabilitiesHandler);
            // This response gets discarded - the listener handles the real response
            return null;
        });
    }
}
