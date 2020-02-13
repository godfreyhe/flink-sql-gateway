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

package com.ververica.flink.table.gateway;

import com.ververica.flink.table.gateway.config.Environment;
import com.ververica.flink.table.gateway.options.GatewayOptions;
import com.ververica.flink.table.gateway.options.GatewayOptionsParser;
import com.ververica.flink.table.gateway.rest.SqlGatewayEndpoint;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.rest.RestServerEndpointConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

/**
 * SqlGateway.
 */
public class SqlGateway {

	private static final Logger LOG = LoggerFactory.getLogger(SqlGateway.class);

	private final GatewayOptions options;
	private SqlGatewayEndpoint endpoint;
	private SessionManager sessionManager;

	public SqlGateway(GatewayOptions options) {
		this.options = options;
	}

	private void start() throws Exception {
		final Environment defaultEnv = readEnvironment(options.getDefaults().orElse(null));

		final Integer port = options.getPort().orElse(defaultEnv.getServer().getPort());
		final String address = defaultEnv.getServer().getAddress();
		final Optional<String> bindAddress = defaultEnv.getServer().getBindAddress();

		Configuration configuration = new Configuration();
		configuration.setString(RestOptions.ADDRESS, address);
		bindAddress.ifPresent(s -> configuration.setString(RestOptions.BIND_ADDRESS, s));
		configuration.setString(RestOptions.BIND_PORT, String.valueOf(port));

		final ExecutorImpl executor = new ExecutorImpl(defaultEnv, options.getJars(), options.getLibraryDirs());
		sessionManager = new SessionManager(defaultEnv, executor);

		endpoint = new SqlGatewayEndpoint(
			RestServerEndpointConfiguration.fromConfiguration(configuration),
			sessionManager);
		endpoint.start();
		System.out.println("Rest endpoint started.");

		new CountDownLatch(1).await();
	}

	private Environment readEnvironment(URL envUrl) {
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
			throw new SqlGatewayException("Could not read session environment file at: " + envUrl, e);
		}
	}

	// --------------------------------------------------------------------------------------------

	public static void main(String[] args) {
		final GatewayOptions options = GatewayOptionsParser.parseGatewayModeGateway(args);
		if (options.isPrintHelp()) {
			GatewayOptionsParser.printHelp();
		} else {
			SqlGateway gateway = new SqlGateway(options);
			try {
				// add shutdown hook
				Runtime.getRuntime().addShutdownHook(new ShutdownThread(gateway));

				// do the actual work
				gateway.start();
			} catch (Throwable t) {
				// make space in terminal
				System.out.println();
				System.out.println();
				LOG.error("Gateway must stop. Unexpected exception. This is a bug. Please consider filing an issue.",
					t);
				throw new SqlGatewayException(
					"Unexpected exception. This is a bug. Please consider filing an issue.", t);
			}
		}
	}

	// --------------------------------------------------------------------------------------------

	private static class ShutdownThread extends Thread {

		private final SqlGateway gateway;

		public ShutdownThread(SqlGateway gateway) {
			this.gateway = gateway;
		}

		@Override
		public void run() {
			// Shutdown the gateway
			System.out.println("\nShutting down the gateway...");
			if (gateway.endpoint != null) {
				try {
					gateway.endpoint.closeAsync();
				} catch (Exception e) {
					LOG.error("Failed to shut down the endpoint: " + e.getMessage());
					System.out.println("Failed to shut down the endpoint: " + e.getMessage());
				}
			}
			if (gateway.sessionManager != null) {
				try {
					gateway.sessionManager.close();
				} catch (Exception e) {
					LOG.error("Failed to shut down the session manger: " + e.getMessage());
					System.out.println("Failed to shut down the session manger: " + e.getMessage());
				}
			}
			System.out.println("done.");
		}
	}
}
