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

package org.elasticsearch.common.rounding;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;

/**
 * A strategy for rounding long values.
 */
public interface Rounding extends Streamable {

    byte id();

    /**
     * Rounds the given value.
     *
     * @param value The value to round.
     * @return      The rounded value.
     */
    long round(long value);

    /**
     * Given the rounded value (which was potentially generated by {@link #round(long)}, returns the next rounding value. For example, with
     * interval based rounding, if the interval is 3, {@code nextRoundValue(6) = 9 }.
     *
     * @param value The current rounding value
     * @return      The next rounding value;
     */
    long nextRoundingValue(long value);

    /**
     * Rounding strategy which is based on an interval
     *
     * {@code rounded = value - (value % interval) }
     */
    public static class Interval implements Rounding {

        final static byte ID = 0;

        private long interval;

        public Interval() { // for serialization
        }

        /**
         * Creates a new interval rounding.
         *
         * @param interval The interval
         */
        public Interval(long interval) {
            this.interval = interval;
        }

        @Override
        public byte id() {
            return ID;
        }

        static long round(long value, long interval) {
            long rem = value % interval;
            // We need this condition because % may return a negative result on negative numbers
            // According to Google caliper's IntModBenchmark, using a condition is faster than attempts to use tricks to avoid
            // the condition. Moreover, in our case, the condition is very likely to be always true (dates, prices, distances),
            // so easily predictable by the CPU
            if (rem < 0) {
                rem += interval;
            }
            return value - rem;
        }

        @Override
        public long round(long value) {
            return round(value, interval);
        }

        @Override
        public long nextRoundingValue(long value) {
            assert value == round(value);
            return value + interval;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            interval = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(interval);
        }
    }

    public static class Streams {

        public static void write(Rounding rounding, StreamOutput out) throws IOException {
            out.writeByte(rounding.id());
            rounding.writeTo(out);
        }

        public static Rounding read(StreamInput in) throws IOException {
            Rounding rounding = null;
            byte id = in.readByte();
            switch (id) {
                case Interval.ID: rounding = new Interval(); break;
                case TimeZoneRounding.TimeTimeZoneRoundingFloor.ID: rounding = new TimeZoneRounding.TimeTimeZoneRoundingFloor(); break;
                case TimeZoneRounding.UTCTimeZoneRoundingFloor.ID: rounding = new TimeZoneRounding.UTCTimeZoneRoundingFloor(); break;
                case TimeZoneRounding.DayTimeZoneRoundingFloor.ID: rounding = new TimeZoneRounding.DayTimeZoneRoundingFloor(); break;
                case TimeZoneRounding.UTCIntervalTimeZoneRounding.ID: rounding = new TimeZoneRounding.UTCIntervalTimeZoneRounding(); break;
                case TimeZoneRounding.TimeIntervalTimeZoneRounding.ID: rounding = new TimeZoneRounding.TimeIntervalTimeZoneRounding(); break;
                case TimeZoneRounding.DayIntervalTimeZoneRounding.ID: rounding = new TimeZoneRounding.DayIntervalTimeZoneRounding(); break;
                case TimeZoneRounding.FactorTimeZoneRounding.ID: rounding = new TimeZoneRounding.FactorTimeZoneRounding(); break;
                case TimeZoneRounding.PrePostTimeZoneRounding.ID: rounding = new TimeZoneRounding.PrePostTimeZoneRounding(); break;
                default: throw new ElasticsearchException("unknown rounding id [" + id + "]");
            }
            rounding.readFrom(in);
            return rounding;
        }

    }

}
