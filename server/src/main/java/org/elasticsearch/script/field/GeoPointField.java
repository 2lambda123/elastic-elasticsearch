/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.elasticsearch.common.geo.GeoPoint;

public class GeoPointField extends Field<GeoPoint> {

    /* ---- Geo Point Field Members ---- */

    public GeoPointField(String name, FieldValues<GeoPoint> values) {
        super(name, values);
    }
}
