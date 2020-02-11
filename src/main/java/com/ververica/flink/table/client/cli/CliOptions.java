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

package com.ververica.flink.table.client.cli;

import javax.annotation.Nullable;

import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Command line options to configure the SQL client. Arguments that have not been specified
 * by the user are null.
 */
public class CliOptions {

	private final boolean isPrintHelp;
	private final String gatewayHost;
	private final Integer gatewayPort;
	private final URL environment;
	private final URL defaults;
	private final List<URL> jars;
	private final List<URL> libraryDirs;
	private final String updateStatement;

	public CliOptions(
		boolean isPrintHelp,
		@Nullable String gatewayHost,
		@Nullable Integer gatewayPort,
		@Nullable URL environment,
		@Nullable URL defaults,
		@Nullable List<URL> jars,
		@Nullable List<URL> libraryDirs,
		@Nullable String updateStatement) {
		this.isPrintHelp = isPrintHelp;
		this.gatewayHost = gatewayHost;
		this.gatewayPort = gatewayPort;
		this.environment = environment;
		this.defaults = defaults;
		this.jars = jars != null ? jars : Collections.emptyList();
		this.libraryDirs = libraryDirs != null ? libraryDirs : Collections.emptyList();
		this.updateStatement = updateStatement;
	}

	public boolean isPrintHelp() {
		return isPrintHelp;
	}

	public Optional<String> getGatewayHost() {
		return Optional.ofNullable(gatewayHost);
	}

	public Optional<Integer> getGatewayPort() {
		return Optional.ofNullable(gatewayPort);
	}

	public Optional<URL> getEnvironment() {
		return Optional.ofNullable(environment);
	}

	public Optional<URL> getDefaults() {
		return Optional.ofNullable(defaults);
	}

	public List<URL> getJars() {
		return jars;
	}

	public List<URL> getLibraryDirs() {
		return libraryDirs;
	}

	public Optional<String> getUpdateStatement() {
		return Optional.ofNullable(updateStatement);
	}
}
