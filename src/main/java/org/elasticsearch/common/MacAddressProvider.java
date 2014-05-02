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

package org.elasticsearch.common;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;


public class MacAddressProvider {

    private static final ESLogger logger = Loggers.getLogger("MacAddressProvider");

    public static byte[] getMacAddress() throws SocketException {
        Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces();
        while (en.hasMoreElements()) {
            NetworkInterface nint = en.nextElement();
            if (!nint.isLoopback()) {
                //Pick the first non loopback address we find
                byte[] address = nint.getHardwareAddress();
                if (address != null && address.length == 6) {
                    return address;
                }
            }
        }
        return null; //Could not find a mac address
    }

    public static byte[] getSecureMungedAddress() {
        byte[] address = null;
        try {
             address = getMacAddress();
        } catch( SocketException se ) {
            logger.warn("Unable to get mac address, will use a dummy address",se);
            address = constructDummyMulticastAddress();
        }

        byte[] mungedBytes = new byte[6];
        RandomBasedUUID.SecureRandomHolder.INSTANCE.nextBytes(mungedBytes);
        for (int i = 0; i < 6; ++i) {
            mungedBytes[i] = (byte) (0xff & (mungedBytes[i] ^ address[i]));
        }

        return mungedBytes;
    }

    private static byte[] constructDummyMulticastAddress() {
        byte[] dummy = new byte[6];
        RandomBasedUUID.SecureRandomHolder.INSTANCE.nextBytes(dummy);
        /*
         * Set the broadcast bit to indicate this is not a _real_ mac address
         */
        dummy[0] |= (byte) 0x01;
        return dummy;
    }


}