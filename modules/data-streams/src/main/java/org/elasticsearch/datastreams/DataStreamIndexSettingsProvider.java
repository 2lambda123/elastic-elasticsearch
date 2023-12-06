/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.datastreams;

import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MappingParserContext;
import org.elasticsearch.index.mapper.TimeSeriesParams;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;

/**
 * An {@link IndexSettingProvider} implementation that adds the index.time_series.start_time,
 * index.time_series.end_time and index.routing_path index settings to backing indices of
 * data streams in time series index mode.
 */
public class DataStreamIndexSettingsProvider implements IndexSettingProvider {

    static final DateFormatter FORMATTER = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;

    private final CheckedFunction<IndexMetadata, MapperService, IOException> mapperServiceFactory;

    DataStreamIndexSettingsProvider(CheckedFunction<IndexMetadata, MapperService, IOException> mapperServiceFactory) {
        this.mapperServiceFactory = mapperServiceFactory;
    }

    @Override
    public Settings getAdditionalIndexSettings(
        String indexName,
        String dataStreamName,
        boolean timeSeries,
        Metadata metadata,
        Instant resolvedAt,
        Settings allSettings,
        List<CompressedXContent> combinedTemplateMappings
    ) {
        if (dataStreamName != null) {
            DataStream dataStream = metadata.dataStreams().get(dataStreamName);
            // First backing index is created and then data stream is rolled over (in a single cluster state update).
            // So at this point we can't check index_mode==time_series,
            // so checking that index_mode==null|standard and templateIndexMode == TIME_SERIES
            boolean migrating = dataStream != null
                && (dataStream.getIndexMode() == null || dataStream.getIndexMode() == IndexMode.STANDARD)
                && timeSeries;
            IndexMode indexMode;
            if (migrating) {
                indexMode = IndexMode.TIME_SERIES;
            } else if (dataStream != null) {
                indexMode = timeSeries ? dataStream.getIndexMode() : null;
            } else if (timeSeries) {
                indexMode = IndexMode.TIME_SERIES;
            } else {
                indexMode = null;
            }
            if (indexMode != null) {
                if (indexMode == IndexMode.TIME_SERIES) {
                    Settings.Builder builder = Settings.builder();
                    TimeValue lookAheadTime = DataStreamsPlugin.LOOK_AHEAD_TIME.get(allSettings);
                    TimeValue lookBackTime = DataStreamsPlugin.LOOK_BACK_TIME.get(allSettings);
                    final Instant start;
                    final Instant end;
                    if (dataStream == null || migrating) {
                        start = DataStream.getCanonicalTimestampBound(resolvedAt.minusMillis(lookBackTime.getMillis()));
                        end = DataStream.getCanonicalTimestampBound(resolvedAt.plusMillis(lookAheadTime.getMillis()));
                    } else {
                        IndexMetadata currentLatestBackingIndex = metadata.index(dataStream.getWriteIndex());
                        if (currentLatestBackingIndex.getSettings().hasValue(IndexSettings.TIME_SERIES_END_TIME.getKey()) == false) {
                            throw new IllegalStateException(
                                String.format(
                                    Locale.ROOT,
                                    "backing index [%s] in tsdb mode doesn't have the [%s] index setting",
                                    currentLatestBackingIndex.getIndex().getName(),
                                    IndexSettings.TIME_SERIES_END_TIME.getKey()
                                )
                            );
                        }
                        start = IndexSettings.TIME_SERIES_END_TIME.get(currentLatestBackingIndex.getSettings());
                        if (start.isAfter(resolvedAt)) {
                            end = DataStream.getCanonicalTimestampBound(start.plusMillis(lookAheadTime.getMillis()));
                        } else {
                            end = DataStream.getCanonicalTimestampBound(resolvedAt.plusMillis(lookAheadTime.getMillis()));
                        }
                    }
                    assert start.isBefore(end) : "data stream backing index's start time is not before end time";
                    builder.put(IndexSettings.TIME_SERIES_START_TIME.getKey(), FORMATTER.format(start));
                    builder.put(IndexSettings.TIME_SERIES_END_TIME.getKey(), FORMATTER.format(end));

                    boolean hasRoutingPath = allSettings.hasValue(IndexMetadata.INDEX_ROUTING_PATH.getKey());
                    boolean useDynamicDimensions = allSettings.getAsBoolean(IndexMetadata.TIME_SERIES_DYNAMIC_TEMPLATES.getKey(), false);
                    if ((hasRoutingPath == false || useDynamicDimensions) && combinedTemplateMappings.isEmpty() == false) {
                        List<String> routingPaths = new ArrayList<>();
                        List<String> dynamicDimensions = new ArrayList<>();
                        findRoutingPathsAndDimensions(
                            indexName,
                            allSettings,
                            combinedTemplateMappings,
                            hasRoutingPath ? null : routingPaths,
                            useDynamicDimensions ? dynamicDimensions : null
                        );
                        if (useDynamicDimensions && hasRoutingPath == false && routingPaths.isEmpty() && dynamicDimensions.isEmpty()) {
                            throw new IllegalStateException(
                                "index "
                                    + indexName
                                    + " with [index.mode=time_series] has no routing path and no dynamic templates with fields marked"
                                    + " as [time_series_dimension]"
                            );
                        }
                        if (routingPaths.isEmpty() == false) {
                            builder.putList(IndexMetadata.INDEX_ROUTING_PATH.getKey(), routingPaths);
                        }
                        if (dynamicDimensions.isEmpty() == false) {
                            builder.putList(IndexMetadata.DYNAMIC_DIMENSION_NAMES.getKey(), dynamicDimensions);
                        }
                    }
                    return builder.build();
                }
            }
        }

        return Settings.EMPTY;
    }

    /**
     * Find fields in mapping that are of type keyword and time_series_dimension enabled.
     * Using MapperService here has an overhead, but allows the mappings from template to
     * be merged correctly and fetching the fields without manually parsing the mappings.
     *
     * Alternatively this method can instead parse mappings into map of maps and merge that and
     * iterate over all values to find the field that can serve as routing value. But this requires
     * mapping specific logic to exist here.
     *
     * In addition to static mappings, dynamic templates can also contain fields annotated as
     * time_series_dimension. These fields are identified and added to a separate list, if one is provided.
     */
    private void findRoutingPathsAndDimensions(
        String indexName,
        Settings allSettings,
        List<CompressedXContent> combinedTemplateMappings,
        @Nullable List<String> routingPaths,
        @Nullable List<String> dynamicDimensions
    ) {
        var tmpIndexMetadata = IndexMetadata.builder(indexName);

        int dummyPartitionSize = IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING.get(allSettings);
        int dummyShards = allSettings.getAsInt(
            IndexMetadata.SETTING_NUMBER_OF_SHARDS,
            dummyPartitionSize == 1 ? 1 : dummyPartitionSize + 1
        );
        int shardReplicas = allSettings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);
        var finalResolvedSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(allSettings)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, dummyShards)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, shardReplicas)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            // Avoid failing in case index.routing_path is empty.
            .put(IndexMetadata.TIME_SERIES_DYNAMIC_TEMPLATES.getKey(), true)
            .build();

        tmpIndexMetadata.settings(finalResolvedSettings);
        // Create MapperService just to extract keyword dimension fields:
        try (var mapperService = mapperServiceFactory.apply(tmpIndexMetadata.build())) {
            mapperService.merge(MapperService.SINGLE_MAPPING_NAME, combinedTemplateMappings, MapperService.MergeReason.INDEX_TEMPLATE);
            if (routingPaths != null) {
                for (var fieldMapper : mapperService.documentMapper().mappers().fieldMappers()) {
                    extractPath(routingPaths, fieldMapper);
                }
            }
            for (var template : mapperService.getAllDynamicTemplates()) {
                var templateName = "__dynamic__" + template.name();
                var mappingSnippet = template.mappingForName(templateName, KeywordFieldMapper.CONTENT_TYPE);
                String mappingSnippetType = (String) mappingSnippet.get("type");
                if (mappingSnippetType == null) {
                    continue;
                }

                if (routingPaths != null) {
                    MappingParserContext parserContext = mapperService.parserContext();
                    for (String pathMatch : template.pathMatch()) {
                        var mapper = parserContext.typeParser(mappingSnippetType)
                            // Since FieldMapper.parse modifies the Map passed in (removing entries for "type"), that means
                            // that only the first pathMatch passed in gets recognized as a time_series_dimension. To counteract
                            // that, we wrap the mappingSnippet in a new HashMap for each pathMatch instance.
                            .parse(pathMatch, new HashMap<>(mappingSnippet), parserContext)
                            .build(MapperBuilderContext.root(false, false));
                        extractPath(routingPaths, mapper);
                    }
                }

                if (dynamicDimensions != null) {
                    Boolean isDimension = (Boolean) mappingSnippet.get(TimeSeriesParams.TIME_SERIES_DIMENSION_PARAM);
                    if (isDimension != null && isDimension) {
                        dynamicDimensions.add(template.name());
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Helper method that adds the name of the mapper to the provided list if it is a keyword dimension field.
     */
    private static void extractPath(List<String> routingPaths, Mapper mapper) {
        if (mapper instanceof KeywordFieldMapper keywordFieldMapper) {
            if (keywordFieldMapper.fieldType().isDimension()) {
                routingPaths.add(mapper.name());
            }
        }
    }

}
