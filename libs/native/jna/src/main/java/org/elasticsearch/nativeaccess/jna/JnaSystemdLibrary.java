/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess.jna;

import org.elasticsearch.nativeaccess.lib.SystemdLibrary;

public class JnaSystemdLibrary implements SystemdLibrary {
    @Override
    public int sd_notify(int unset_environment, String state) {
        return JnaStaticSystemdLibrary.sd_notify(unset_environment, state);
    }
}
