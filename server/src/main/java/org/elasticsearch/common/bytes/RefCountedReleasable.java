/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.bytes;

import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

public final class RefCountedReleasable extends AbstractRefCounted {

    private final Releasable releasable;

    public RefCountedReleasable(Releasable releasable) {
        this.releasable = releasable;
    }

    @Override
    protected void closeInternal() {
        Releasables.closeExpectNoException(releasable);
    }
}
