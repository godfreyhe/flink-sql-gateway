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

import com.ververica.flink.table.client.SqlClientException;
import com.ververica.flink.table.client.cli.CliOptions;
import com.ververica.flink.table.client.cli.CliOptionsParser;
import com.ververica.flink.table.config.Environment;
import com.ververica.flink.table.config.EnvironmentUtils;
import com.ververica.flink.table.rest.SqlClientGatewayEndpoint;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.rest.RestServerEndpointConfiguration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;

/**
 * SQL Client for submitting SQL statements. The client can be executed in two
 * modes: a gateway and embedded mode.
 *
 * <p>- In embedded mode, the SQL CLI is tightly coupled with the executor in a common process. This
 * allows for submitting jobs without having to start an additional component.
 *
 * <p>- In future versions: In gateway mode, the SQL CLI client connects to the REST API of the gateway
 * and allows for managing queries via console.
 *
 * <p>For debugging in an IDE you can execute the main method of this class using:
 * "embedded --defaults /path/to/sql-client-defaults.yaml --jar /path/to/target/flink-sql-client-*.jar"
 *
 * <p>Make sure that the FLINK_CONF_DIR environment variable is set.
 */
public class SqlGateway {

	private static final Logger LOG = LoggerFactory.getLogger(SqlGateway.class);

	private final CliOptions options;
	private SqlClientGatewayEndpoint endpoint;

	public SqlGateway(CliOptions options) {
		this.options = options;
	}

	private void start() throws Exception {
		final Environment defaultEnv =
			EnvironmentUtils.readDefaultGatewayEnvironment(options.getDefaults().orElse(null));

		final Integer port = options.getGatewayPort().orElse(defaultEnv.getGateway().getPort());
		final String address = options.getGatewayHost().orElse(defaultEnv.getGateway().getAddress());
		final Optional<String> bindAddress = defaultEnv.getGateway().getBindAddress();

		Configuration configuration = new Configuration();
		configuration.setString(RestOptions.ADDRESS, address);
		bindAddress.ifPresent(s -> configuration.setString(RestOptions.BIND_ADDRESS, s));
		configuration.setString(RestOptions.BIND_PORT, String.valueOf(port));

		final ExecutorImpl executor = new ExecutorImpl(defaultEnv, options.getJars(), options.getLibraryDirs());
		final SessionManager sessionManager = new SessionManager(defaultEnv, executor);

		endpoint = new SqlClientGatewayEndpoint(
			RestServerEndpointConfiguration.fromConfiguration(configuration),
			sessionManager);
		endpoint.start();
		System.out.println("Rest endpoint started.");

		new CountDownLatch(1).await();
	}

	// --------------------------------------------------------------------------------------------

	public static void main(String[] args) {
		final CliOptions options = CliOptionsParser.parseGatewayModeGateway(args);
		if (options.isPrintHelp()) {
			CliOptionsParser.printHelpGatewayModeGateway();
		} else {
			SqlGateway gateway = new SqlGateway(options);
			try {
				// add shutdown hook
				Runtime.getRuntime().addShutdownHook(new GatewayShutdownThread(gateway));

				// do the actual work
				gateway.start();
			} catch (Throwable t) {
				// make space in terminal
				System.out.println();
				System.out.println();
				LOG.error("Gateway must stop. Unexpected exception. This is a bug. Please consider filing an issue.",
					t);
				throw new SqlClientException(
					"Unexpected exception. This is a bug. Please consider filing an issue.", t);
			}
		}
	}

	// --------------------------------------------------------------------------------------------

	private static class GatewayShutdownThread extends Thread {

		private final SqlGateway gateway;

		public GatewayShutdownThread(SqlGateway gateway) {
			this.gateway = gateway;
		}

		@Override
		public void run() {
			// Shutdown the executor
			System.out.println("\nShutting down the gateway...");
			if (gateway.endpoint != null) {
				try {
					gateway.endpoint.closeAsync();
				} catch (Exception e) {
					throw new SqlClientException(e);
				}
			}
			System.out.println("done.");
		}
	}
}
