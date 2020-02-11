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

package com.ververica.flink.table.client;

import com.ververica.flink.table.client.cli.CliClient;
import com.ververica.flink.table.client.cli.CliOptions;
import com.ververica.flink.table.client.cli.CliOptionsParser;
import com.ververica.flink.table.config.Environment;
import com.ververica.flink.table.config.EnvironmentUtils;
import com.ververica.flink.table.gateway.Executor;
import com.ververica.flink.table.rest.SessionClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * SQL Client for submitting SQL statements. The client can be executed in two
 * modes: a gateway and embedded mode.
 *
 * <p>- In embedded mode, the SQL CLI is tightly coupled with the executor in a common process. This
 * allows for submitting jobs without having to start an additional component.
 *
 * <p>- In gateway mode, the SQL CLI client connects to the REST API of the gateway
 * and allows for managing queries via console.
 *
 * <p>For debugging in an IDE you can execute the main method of this class using:
 * "embedded --defaults /path/to/sql-client-defaults.yaml --jar /path/to/target/flink-sql-client-*.jar"
 *
 * <p>Make sure that the FLINK_CONF_DIR environment variable is set.
 */
public class SqlClient {

	private static final Logger LOG = LoggerFactory.getLogger(SqlClient.class);

	private final String mode;
	private final CliOptions options;

	public static final String MODE_EMBEDDED = "embedded";
	public static final String MODE_GATEWAY = "gateway";

	public SqlClient(String mode, CliOptions options) {
		this.mode = mode;
		this.options = options;
	}

	private void start() {
		switch (mode) {
			case MODE_EMBEDDED:
				throw new UnsupportedOperationException("Embedded mode is unsupported now.");
			case MODE_GATEWAY:
				// create CLI client with session environment
				final Environment gatewayClientSessionEnv =
					EnvironmentUtils.readSessionEnvironment(options.getEnvironment().orElse(null));
				final Integer port = options.getGatewayPort().orElse(
					gatewayClientSessionEnv.getGateway().getPort());
				final String address = options.getGatewayHost().orElse(
					gatewayClientSessionEnv.getGateway().getAddress());

				SessionClient session;
				try {
					session = new SessionClient(
						address,
						port,
						gatewayClientSessionEnv.getSession().getSessionName().orElse("Gateway-CliClient"),
						gatewayClientSessionEnv.getExecution().getPlanner(),
						gatewayClientSessionEnv.getExecution().getExecutionMode(),
						"Flink-CliClient-Gateway-Connection-IO");
				} catch (Exception e) {
					throw new SqlClientException("Create session failed", e);
				}

				try {
					Runtime.getRuntime().addShutdownHook(new GatewayClientShutdownThread(session));
					// do the actual work
					openCli(session, gatewayClientSessionEnv);
				} finally {
					try {
						session.close();
					} catch (Exception e) {
						System.err.println("Close session client failed: " + e.getMessage());
						LOG.warn("Close session client failed.", e);
					}
				}

				break;
		}
	}


	private void openCli(SessionClient client, Environment environment) {
		CliClient cli = null;
		try {
			cli = new CliClient(client, environment);
			// interactive CLI mode
			if (!options.getUpdateStatement().isPresent()) {
				cli.open();
			}
			// execute single update statement
			else {
				final boolean success = cli.submitUpdate(options.getUpdateStatement().get());
				if (!success) {
					throw new SqlClientException("Could not submit given SQL update statement to cluster.");
				}
			}
		} finally {
			if (cli != null) {
				cli.close();
			}
		}
	}

	// --------------------------------------------------------------------------------------------

	public static void main(String[] args) {
		if (args.length < 1) {
			CliOptionsParser.printHelpClient();
			return;
		}

		String mode = args[0].toLowerCase();
		// remove mode
		final String[] argsWithoutMode = Arrays.copyOfRange(args, 1, args.length);
		final CliOptions options;

		switch (mode) {
			case MODE_EMBEDDED:
				options = CliOptionsParser.parseEmbeddedModeClient(argsWithoutMode);
				if (options.isPrintHelp()) {
					CliOptionsParser.printHelpEmbeddedModeClient();
					return;
				}
				break;

			case MODE_GATEWAY:
				options = CliOptionsParser.parseGatewayModeClient(argsWithoutMode);
				if (options.isPrintHelp()) {
					CliOptionsParser.printHelpGatewayModeClient();
					return;
				}
				break;
			default:
				CliOptionsParser.printHelpClient();
				return;
		}
		try {
			final SqlClient client = new SqlClient(mode, options);
			client.start();
		} catch (SqlClientException e) {
			// make space in terminal
			System.out.println();
			System.out.println();
			LOG.error("SQL Client must stop.", e);
			throw e;
		} catch (Throwable t) {
			// make space in terminal
			System.out.println();
			System.out.println();
			LOG.error(
				"SQL Client must stop. Unexpected exception. This is a bug. Please consider filing an issue.",
				t);
			throw new SqlClientException(
				"Unexpected exception. This is a bug. Please consider filing an issue.", t);
		}
	}

	// --------------------------------------------------------------------------------------------

	private static class EmbeddedShutdownThread extends Thread {

		private final String sessionId;
		private final Executor executor;

		public EmbeddedShutdownThread(String sessionId, Executor executor) {
			this.sessionId = sessionId;
			this.executor = executor;
		}

		@Override
		public void run() {
			// Shutdown the executor
			System.out.println("\nShutting down the session...");
			executor.closeSession(sessionId);
			System.out.println("done.");
		}
	}

	private static class GatewayClientShutdownThread extends Thread {

		private final SessionClient session;

		public GatewayClientShutdownThread(SessionClient session) {
			this.session = session;
		}

		@Override
		public void run() {
			// Shutdown the executor
			System.out.println("\nShutting down the session...");
			try {
				session.close();
			} catch (Exception e) {
				System.err.println("Close session client failed: " + e.getMessage());
				LOG.warn("Close session client failed.", e);
			}
			System.out.println("done.");
		}
	}
}
