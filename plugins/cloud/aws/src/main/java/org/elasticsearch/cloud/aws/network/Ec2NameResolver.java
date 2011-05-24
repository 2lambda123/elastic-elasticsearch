/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.cloud.aws.network;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.URL;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.network.NetworkService.CustomNameResolver;

/**
 * Resolves certain ec2 related 'meta' hostnames into an actual hostname
 * obtained from ec2 meta-data.
 * <p /> 
 * Valid config values for {@link Ec2HostnameType}s are -
 * <ul>
 *     <li>_ec2_ - maps to privateIpv4</li>
 *     <li>_ec2:privateIp_ - maps to privateIpv4</li>
 *     <li>_ec2:privateIpv4_</li>
 *     <li>_ec2:privateDns_</li>
 *     <li>_ec2:publicIp_ - maps to publicIpv4</li>
 *     <li>_ec2:publicIpv4_</li>
 *     <li>_ec2:publicDns_</li>
 * </ul>
 * @author Paul_Loy (keteracel)
 */
public class Ec2NameResolver implements CustomNameResolver {

	/**
	 *  enum that can be added to over time with more meta-data types (such as ipv6 when this is available)
	 *  
	 * @author Paul_Loy
	 */
	private static enum Ec2HostnameType {

		PRIVATE_IPv4("ec2:privateIpv4", "local-ipv4"),
		PRIVATE_DNS ("ec2:privateDns",  "local-hostname"),
		PUBLIC_IPv4 ("ec2:publicIpv4",  "public-ipv4"),
		PUBLIC_DNS  ("ec2:publicDns",   "public-hostname"),

		// some less verbose defaults
		PUBLIC_IP   ("ec2:publicIp",    PUBLIC_IPv4.ec2Name),
		PRIVATE_IP  ("ec2:privateIp",   PRIVATE_IPv4.ec2Name),
		DEFAULT     ("ec2",             PRIVATE_IPv4.ec2Name);

		final String configName;
		final String ec2Name;

		private Ec2HostnameType(String configName, String ec2Name) {
			this.configName = configName;
			this.ec2Name = ec2Name;
		}

	}

	private static final String EC2_METADATA_URL = "http://169.254.169.254/latest/meta-data/";
	private final ESLogger logger;

	/**
	 * Construct a {@link CustomNameResolver}.
	 * 
	 */
	public Ec2NameResolver() {
		logger = Loggers.getLogger(getClass());
	}

	/**
	 * @param type the ec2 hostname type to discover.
	 * @return the appropriate host resolved from ec2 meta-data.
	 * @throws IOException if ec2 meta-data cannot be obtained.
	 * 
	 * @see CustomNameResolver#resolveIfPossible(String)
	 */
	public InetAddress resolve(Ec2HostnameType type) {
		try {
			URL url = new URL(EC2_METADATA_URL + type.ec2Name);
			logger.info("obtaining ec2 hostname from ec2 meta-data url {}", url);
			BufferedReader urlReader = new BufferedReader(new InputStreamReader(url.openStream()));

			String metadataResult = urlReader.readLine();
			if (metadataResult == null || metadataResult.length() == 0) {
				logger.error("no ec2 metadata returned from {}", url);
				return null;
			}
			return InetAddress.getByName(metadataResult);
		} catch (IOException e) {
			logger.error("exception obtaining metadata", e);
			return null;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.elasticsearch.common.network.NetworkService.CustomNameResolver#resolveDefault()
	 */
	@Override
	public InetAddress resolveDefault() {
		return resolve(Ec2HostnameType.DEFAULT);
	}

	/*
	 * (non-Javadoc)
	 * @see org.elasticsearch.common.network.NetworkService.CustomNameResolver#resolveIfPossible(java.lang.String)
	 */
	@Override
	public InetAddress resolveIfPossible(String value) {
		for (Ec2HostnameType type : Ec2HostnameType.values()) {
			if (type.configName.equals(value)) {
				return resolve(type);
			}
		}
		return null;
	}

}
