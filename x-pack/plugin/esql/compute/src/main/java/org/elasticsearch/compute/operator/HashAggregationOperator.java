/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.Describable;
import org.elasticsearch.compute.aggregation.GroupingAggregator;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class HashAggregationOperator implements Operator {

    public record GroupSpec(int channel, ElementType elementType) {}

    public record HashAggregationOperatorFactory(List<GroupSpec> groups, List<GroupingAggregator.Factory> aggregators, int maxPageSize)
        implements
            OperatorFactory {
        @Override
        public Operator get(DriverContext driverContext) {
            return new HashAggregationOperator(
                aggregators,
                () -> BlockHash.build(groups, driverContext, maxPageSize, false),
                driverContext
            );
        }

        @Override
        public String describe() {
            return "HashAggregationOperator[mode = "
                + "<not-needed>"
                + ", aggs = "
                + aggregators.stream().map(Describable::describe).collect(joining(", "))
                + "]";
        }
    }

    private boolean finished;
    private Page output;

    private final BlockHash blockHash;

    private final List<GroupingAggregator> aggregators;

    private final DriverContext driverContext;

    private long hashNanos;
    private long aggregationNanos;
    private int pagesProcessed;

    @SuppressWarnings("this-escape")
    public HashAggregationOperator(
        List<GroupingAggregator.Factory> aggregators,
        Supplier<BlockHash> blockHash,
        DriverContext driverContext
    ) {
        this.aggregators = new ArrayList<>(aggregators.size());
        this.driverContext = driverContext;
        boolean success = false;
        try {
            this.blockHash = blockHash.get();
            for (GroupingAggregator.Factory a : aggregators) {
                this.aggregators.add(a.apply(driverContext));
            }
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    @Override
    public boolean needsInput() {
        return finished == false;
    }

    @Override
    public void addInput(Page page) {
        try {
            checkState(needsInput(), "Operator is already finishing");
            requireNonNull(page, "page is null");

            GroupingAggregatorFunction.AddInput[] prepared = new GroupingAggregatorFunction.AddInput[aggregators.size()];
            for (int i = 0; i < prepared.length; i++) {
                prepared[i] = aggregators.get(i).prepareProcessPage(blockHash, page);
            }

            class AddInput implements GroupingAggregatorFunction.AddInput {
                long hashStart = System.nanoTime();
                long aggStart;

                @Override
                public void add(int positionOffset, IntBlock groupIds) {
                    IntVector groupIdsVector = groupIds.asVector();
                    if (groupIdsVector != null) {
                        add(positionOffset, groupIdsVector);
                    } else {
                        start();
                        for (GroupingAggregatorFunction.AddInput p : prepared) {
                            p.add(positionOffset, groupIds);
                        }
                        end();
                    }
                }

                @Override
                public void add(int positionOffset, IntVector groupIds) {
                    start();
                    for (GroupingAggregatorFunction.AddInput p : prepared) {
                        p.add(positionOffset, groupIds);
                    }
                    end();
                }

                private void start() {
                    aggStart = System.nanoTime();
                    hashNanos += aggStart - hashStart;
                }

                private void end() {
                    hashStart = System.nanoTime();
                    aggregationNanos += hashStart - aggStart;
                }
            }
            AddInput add = new AddInput();
            blockHash.add(wrapPage(page), add);
            hashNanos += add.hashStart - System.nanoTime();
        } finally {
            page.releaseBlocks();
        }
    }

    @Override
    public Page getOutput() {
        Page p = output;
        output = null;
        return p;
    }

    @Override
    public void finish() {
        if (finished) {
            return;
        }
        finished = true;
        Block[] blocks = null;
        IntVector selected = null;
        boolean success = false;
        try {
            selected = blockHash.nonEmpty();
            Block[] keys = blockHash.getKeys();
            int[] aggBlockCounts = aggregators.stream().mapToInt(GroupingAggregator::evaluateBlockCount).toArray();
            blocks = new Block[keys.length + Arrays.stream(aggBlockCounts).sum()];
            System.arraycopy(keys, 0, blocks, 0, keys.length);
            int offset = keys.length;
            for (int i = 0; i < aggregators.size(); i++) {
                var aggregator = aggregators.get(i);
                aggregator.evaluate(blocks, offset, selected, driverContext);
                offset += aggBlockCounts[i];
            }
            output = new Page(blocks);
            success = true;
        } finally {
            // selected should always be closed
            if (selected != null) {
                selected.close();
            }
            if (success == false && blocks != null) {
                Releasables.closeExpectNoException(blocks);
            }
        }
    }

    @Override
    public boolean isFinished() {
        return finished && output == null;
    }

    @Override
    public void close() {
        if (output != null) {
            output.releaseBlocks();
        }
        Releasables.close(blockHash, () -> Releasables.close(aggregators));
    }

    @Override
    public Operator.Status status() {
        return new Status(hashNanos, aggregationNanos, pagesProcessed);
    }

    protected static void checkState(boolean condition, String msg) {
        if (condition == false) {
            throw new IllegalArgumentException(msg);
        }
    }

    protected Page wrapPage(Page page) {
        return page;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName()).append("[");
        sb.append("blockHash=").append(blockHash).append(", ");
        sb.append("aggregators=").append(aggregators);
        sb.append("]");
        return sb.toString();
    }

    public static class Status implements Operator.Status {
        public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
            Operator.Status.class,
            "hashagg",
            Status::new
        );

        private final long hashNanos;
        private final long aggregationNanos;
        private final int pagesProcessed;

        public Status(long hashNanos, long aggregationNanos, int pagesProcessed) {
            this.hashNanos = hashNanos;
            this.aggregationNanos = aggregationNanos;
            this.pagesProcessed = pagesProcessed;
        }

        protected Status(StreamInput in) throws IOException {
            hashNanos = in.readVLong();
            aggregationNanos = in.readVLong();
            pagesProcessed = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(hashNanos);
            out.writeVLong(aggregationNanos);
            out.writeVInt(pagesProcessed);
        }

        @Override
        public String getWriteableName() {
            return ENTRY.name;
        }

        public long hashNanos() {
            return hashNanos;
        }

        public long aggregationNanos() {
            return aggregationNanos;
        }

        public int pagesProcessed() {
            return pagesProcessed;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("hash_nanos", hashNanos);
            if (builder.humanReadable()) {
                builder.field("hash_time", TimeValue.timeValueNanos(hashNanos));
            }
            builder.field("aggregation_nanos", aggregationNanos);
            if (builder.humanReadable()) {
                builder.field("aggregation_time", TimeValue.timeValueNanos(aggregationNanos));
            }
            builder.field("pages_processed", pagesProcessed);
            return builder.endObject();

        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Status status = (Status) o;
            return hashNanos == status.hashNanos && aggregationNanos == status.aggregationNanos && pagesProcessed == status.pagesProcessed;
        }

        @Override
        public int hashCode() {
            return Objects.hash(hashNanos, aggregationNanos, pagesProcessed);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }
}
