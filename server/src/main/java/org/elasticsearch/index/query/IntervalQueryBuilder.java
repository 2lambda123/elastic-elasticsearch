package org.elasticsearch.index.query;

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.intervals.IntervalQuery;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class IntervalQueryBuilder extends AbstractQueryBuilder<IntervalQueryBuilder> {

    public static final String NAME = "intervals";

    private final String field;
    private final IntervalsSourceProvider sourceProvider;

    public IntervalQueryBuilder(String field, IntervalsSourceProvider sourceProvider) {
        this.field = field;
        this.sourceProvider = sourceProvider;
    }

    public IntervalQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.field = in.readString();
        this.sourceProvider = in.readNamedWriteable(IntervalsSourceProvider.class);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(field);
        out.writeNamedWriteable(sourceProvider);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field("field", field);
        builder.field("source", sourceProvider);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    private static final ConstructingObjectParser<IntervalQueryBuilder, Void> PARSER
        = new ConstructingObjectParser<>(NAME, args -> new IntervalQueryBuilder((String) args[0], (IntervalsSourceProvider) args[1]));
    static {
        PARSER.declareString(constructorArg(), new ParseField("field"));
        PARSER.declareObject(constructorArg(), (parser, c) -> IntervalsSourceProvider.fromXContent(parser), new ParseField("source"));
        PARSER.declareFloat(IntervalQueryBuilder::boost, new ParseField("boost"));
        PARSER.declareString(IntervalQueryBuilder::queryName, new ParseField("_name"));
    }

    public static IntervalQueryBuilder fromXContent(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        MappedFieldType fieldType = context.fieldMapper(field);
        if (fieldType == null) {
            throw new IllegalArgumentException("Cannot create IntervalQuery over non-existent field [" + field + "]");
        }
        if (fieldType.tokenized() == false ||
            fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0) {
            throw new IllegalArgumentException("Cannot create IntervalQuery over field [" + field + "] with no indexed positions");
        }
        return new IntervalQuery(field, sourceProvider.getSource(fieldType));
    }

    @Override
    protected boolean doEquals(IntervalQueryBuilder other) {
        return Objects.equals(field, other.field) && Objects.equals(sourceProvider, other.sourceProvider);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(field, sourceProvider);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
