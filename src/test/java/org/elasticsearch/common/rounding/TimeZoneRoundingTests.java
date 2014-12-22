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

package org.elasticsearch.common.rounding;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.joda.time.DateTimeZone;
import org.joda.time.format.ISODateTimeFormat;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;

/**
 */
public class TimeZoneRoundingTests extends ElasticsearchTestCase {

    @Test
    public void testUTCMonthRounding() {
        Rounding tzRounding = TimeZoneRounding.builder(DateTimeUnit.MONTH_OF_YEAR).build();
        assertThat(tzRounding.round(utc("2009-02-03T01:01:01")), equalTo(utc("2009-02-01T00:00:00.000Z")));
        assertThat(tzRounding.nextRoundingValue(utc("2009-02-01T00:00:00.000Z")), equalTo(utc("2009-03-01T00:00:00.000Z")));

        tzRounding = TimeZoneRounding.builder(DateTimeUnit.MONTH_OF_YEAR).preZone(DateTimeZone.forOffsetHours(-2)).postZone(DateTimeZone.forOffsetHours(-2)).build();
        assertThat(tzRounding.round(utc("2009-02-03T01:01:01")), equalTo(utc("2009-01-31T22:00:00.000Z")));
        assertThat(tzRounding.nextRoundingValue(utc("2009-09-30T22:00:00.000Z")), equalTo(utc("2009-10-31T22:00:00.000Z")));
        
        tzRounding = TimeZoneRounding.builder(DateTimeUnit.WEEK_OF_WEEKYEAR).build();
        assertThat(tzRounding.round(utc("2012-01-10T01:01:01")), equalTo(utc("2012-01-09T00:00:00.000Z")));
        assertThat(tzRounding.nextRoundingValue(utc("2012-01-09T00:00:00.000Z")), equalTo(utc("2012-01-16T00:00:00.000Z")));

        tzRounding = TimeZoneRounding.builder(DateTimeUnit.WEEK_OF_WEEKYEAR).postOffset(-TimeValue.timeValueHours(24).millis()).build();
        assertThat(tzRounding.round(utc("2012-01-10T01:01:01")), equalTo(utc("2012-01-08T00:00:00.000Z")));
        assertThat(tzRounding.nextRoundingValue(utc("2012-01-08T00:00:00.000Z")), equalTo(utc("2012-01-15T00:00:00.000Z")));
    }

    @Test
    public void testDayTimeZoneRounding() {
        Rounding tzRounding = TimeZoneRounding.builder(DateTimeUnit.DAY_OF_MONTH).preZone(DateTimeZone.forOffsetHours(-2)).build();
        assertThat(tzRounding.round(0), equalTo(0l - TimeValue.timeValueHours(24).millis()));
        assertThat(tzRounding.nextRoundingValue(0l - TimeValue.timeValueHours(24).millis()), equalTo(0l));

        tzRounding = TimeZoneRounding.builder(DateTimeUnit.DAY_OF_MONTH).preZone(DateTimeZone.forOffsetHours(-2)).postZone(DateTimeZone.forOffsetHours(-2)).build();
        assertThat(tzRounding.round(0), equalTo(0l - TimeValue.timeValueHours(26).millis()));
        assertThat(tzRounding.nextRoundingValue(0l - TimeValue.timeValueHours(26).millis()), equalTo(-TimeValue.timeValueHours(2).millis()));

        tzRounding = TimeZoneRounding.builder(DateTimeUnit.DAY_OF_MONTH).preZone(DateTimeZone.forOffsetHours(-2)).build();
        assertThat(tzRounding.round(utc("2009-02-03T01:01:01")), equalTo(utc("2009-02-02T00:00:00")));
        assertThat(tzRounding.nextRoundingValue(utc("2009-02-02T00:00:00")), equalTo(utc("2009-02-03T00:00:00")));

        tzRounding = TimeZoneRounding.builder(DateTimeUnit.DAY_OF_MONTH).preZone(DateTimeZone.forOffsetHours(-2)).postZone(DateTimeZone.forOffsetHours(-2)).build();
        assertThat(tzRounding.round(utc("2009-02-03T01:01:01")), equalTo(time("2009-02-02T00:00:00", DateTimeZone.forOffsetHours(+2))));
        assertThat(tzRounding.nextRoundingValue(time("2009-02-02T00:00:00", DateTimeZone.forOffsetHours(+2))), equalTo(time("2009-02-03T00:00:00", DateTimeZone.forOffsetHours(+2))));
        
    }

    @Test
    public void testTimeTimeZoneRounding() {
        Rounding tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).preZone(DateTimeZone.forOffsetHours(-2)).build();
        assertThat(tzRounding.round(0), equalTo(0l));
        assertThat(tzRounding.nextRoundingValue(0l), equalTo(TimeValue.timeValueHours(1l).getMillis()));

        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).preZone(DateTimeZone.forOffsetHours(-2)).postZone(DateTimeZone.forOffsetHours(-2)).build();
        assertThat(tzRounding.round(0), equalTo(0l - TimeValue.timeValueHours(2).millis()));
        assertThat(tzRounding.nextRoundingValue(0l - TimeValue.timeValueHours(2).millis()), equalTo(0l - TimeValue.timeValueHours(1).millis()));

        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).preZone(DateTimeZone.forOffsetHours(-2)).build();
        assertThat(tzRounding.round(utc("2009-02-03T01:01:01")), equalTo(utc("2009-02-03T01:00:00")));
        assertThat(tzRounding.nextRoundingValue(utc("2009-02-03T01:00:00")), equalTo(utc("2009-02-03T02:00:00")));

        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).preZone(DateTimeZone.forOffsetHours(-2)).postZone(DateTimeZone.forOffsetHours(-2)).build();
        assertThat(tzRounding.round(utc("2009-02-03T01:01:01")), equalTo(time("2009-02-03T01:00:00", DateTimeZone.forOffsetHours(+2))));
        assertThat(tzRounding.nextRoundingValue(time("2009-02-03T01:00:00", DateTimeZone.forOffsetHours(+2))), equalTo(time("2009-02-03T02:00:00", DateTimeZone.forOffsetHours(+2))));

    }
    
    @Test
    public void testTimeTimeZoneRoundingDST() {
        Rounding tzRounding;
        // testing savings to non savings switch 
        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).preZone(DateTimeZone.forID("UTC")).build();
        assertThat(tzRounding.round(time("2014-10-26T01:01:01", DateTimeZone.forID("CET"))), equalTo(time("2014-10-26T01:00:00", DateTimeZone.forID("CET"))));
        
        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).preZone(DateTimeZone.forID("CET")).build();
        assertThat(tzRounding.round(time("2014-10-26T01:01:01", DateTimeZone.forID("CET"))), equalTo(time("2014-10-26T01:00:00", DateTimeZone.forID("CET"))));
        
        // testing non savings to savings switch
        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).preZone(DateTimeZone.forID("UTC")).build();
        assertThat(tzRounding.round(time("2014-03-30T01:01:01", DateTimeZone.forID("CET"))), equalTo(time("2014-03-30T01:00:00", DateTimeZone.forID("CET"))));
        
        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).preZone(DateTimeZone.forID("CET")).build();
        assertThat(tzRounding.round(time("2014-03-30T01:01:01", DateTimeZone.forID("CET"))), equalTo(time("2014-03-30T01:00:00", DateTimeZone.forID("CET"))));
        
        // testing non savings to savings switch (America/Chicago)
        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).preZone(DateTimeZone.forID("UTC")).build();
        assertThat(tzRounding.round(time("2014-03-09T03:01:01", DateTimeZone.forID("America/Chicago"))), equalTo(time("2014-03-09T03:00:00", DateTimeZone.forID("America/Chicago"))));
        
        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).preZone(DateTimeZone.forID("America/Chicago")).build();
        assertThat(tzRounding.round(time("2014-03-09T03:01:01", DateTimeZone.forID("America/Chicago"))), equalTo(time("2014-03-09T03:00:00", DateTimeZone.forID("America/Chicago"))));
        
        // testing savings to non savings switch 2013 (America/Chicago)
        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).preZone(DateTimeZone.forID("UTC")).build();
        assertThat(tzRounding.round(time("2013-11-03T06:01:01", DateTimeZone.forID("America/Chicago"))), equalTo(time("2013-11-03T06:00:00", DateTimeZone.forID("America/Chicago"))));
        
        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).preZone(DateTimeZone.forID("America/Chicago")).build();
        assertThat(tzRounding.round(time("2013-11-03T06:01:01", DateTimeZone.forID("America/Chicago"))), equalTo(time("2013-11-03T06:00:00", DateTimeZone.forID("America/Chicago"))));
        
        // testing savings to non savings switch 2014 (America/Chicago)
        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).preZone(DateTimeZone.forID("UTC")).build();
        assertThat(tzRounding.round(time("2014-11-02T06:01:01", DateTimeZone.forID("America/Chicago"))), equalTo(time("2014-11-02T06:00:00", DateTimeZone.forID("America/Chicago"))));
        
        tzRounding = TimeZoneRounding.builder(DateTimeUnit.HOUR_OF_DAY).preZone(DateTimeZone.forID("America/Chicago")).build();
        assertThat(tzRounding.round(time("2014-11-02T06:01:01", DateTimeZone.forID("America/Chicago"))), equalTo(time("2014-11-02T06:00:00", DateTimeZone.forID("America/Chicago"))));
    }

    private long utc(String time) {
        return time(time, DateTimeZone.UTC);
    }

    private long time(String time, DateTimeZone zone) {
        return ISODateTimeFormat.dateOptionalTimeParser().withZone(zone).parseMillis(time);
    }
}
