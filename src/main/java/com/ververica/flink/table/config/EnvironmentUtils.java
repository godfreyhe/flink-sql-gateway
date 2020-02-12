/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.flink.table.config;

import com.ververica.flink.table.client.SqlClientException;

import org.apache.flink.client.cli.CliFrontend;
import org.apache.flink.core.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

public class EnvironmentUtils {

	private static final Logger LOG = LoggerFactory.getLogger(EnvironmentUtils.class);

	private static final String DEFAULT_CLIENT_ENV_FILE = "sql-client-defaults.yaml";

	private static final String DEFAULT_GATEWAY_ENV_FILE = "sql-gateway-defaults.yaml";

	public static Environment readDefaultClientEnvironment(URL defaultEnv) {
		return readDefaultEnvironment(defaultEnv, DEFAULT_CLIENT_ENV_FILE);
	}

	public static Environment readDefaultGatewayEnvironment(URL defaultEnv) {
		return readDefaultEnvironment(defaultEnv, DEFAULT_GATEWAY_ENV_FILE);
	}

	private static Environment readDefaultEnvironment(URL defaultEnv, String envFile) {
		// TODO remove flink config dir
		String flinkConfigDir = CliFrontend.getConfigurationDirectoryFromEnv();
		// try to find a default environment
		if (defaultEnv == null) {
			final String defaultFilePath = flinkConfigDir + "/" + envFile;
			System.out.println("No default environment specified.");
			System.out.print("Searching for '" + defaultFilePath + "'...");
			final File file = new File(defaultFilePath);
			if (file.exists()) {
				System.out.println("found.");
				try {
					defaultEnv = Path.fromLocalFile(file).toUri().toURL();
				} catch (MalformedURLException e) {
					throw new SqlClientException(e);
				}
				LOG.info("Using default environment file: {}", defaultEnv);
			} else {
				System.out.println("not found.");
			}
		}

		// inform user
		if (defaultEnv != null) {
			System.out.println("Reading default environment from: " + defaultEnv);
			try {
				return Environment.parse(defaultEnv);
			} catch (IOException e) {
				throw new SqlClientException("Could not read default environment file at: " + defaultEnv, e);
			}
		} else {
			return new Environment();
		}
	}

	public static Environment readSessionEnvironment(URL envUrl) {
		// use an empty environment by default
		if (envUrl == null) {
			System.out.println("No session environment specified.");
			return new Environment();
		}

		System.out.println("Reading session environment from: " + envUrl);
		LOG.info("Using session environment file: {}", envUrl);
		try {
			return Environment.parse(envUrl);
		} catch (IOException e) {
			throw new SqlClientException("Could not read session environment file at: " + envUrl, e);
		}
	}
}
