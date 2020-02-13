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

package com.ververica.flink.table.gateway.result;

import com.ververica.flink.table.gateway.SqlExecutionException;
import com.ververica.flink.table.gateway.SqlGatewayException;
import com.ververica.flink.table.gateway.config.Environment;
import com.ververica.flink.table.gateway.config.entries.DeploymentEntry;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.net.ConnectionUtils;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Maintains dynamic results.
 */
public class ResultStore {

	private final Configuration flinkConfig;
	private final Map<String, DynamicResult<?>> results;

	public ResultStore(Configuration flinkConfig) {
		this.flinkConfig = flinkConfig;
		results = new HashMap<>();
	}

	/**
	 * Creates a result. Might start threads or opens sockets so every created result must be closed.
	 */
	public <T> DynamicResult<T> createResult(
		Environment env,
		TableSchema schema,
		ExecutionConfig config,
		ClassLoader classLoader) {

		final TypeInformation[] schemaTypeInfos = Stream.of(schema.getFieldDataTypes())
				.map(TypeInfoDataTypeConverter::fromDataTypeToTypeInfo)
				.toArray(TypeInformation[]::new);
		final RowTypeInfo outputType = new RowTypeInfo(schemaTypeInfos, schema.getFieldNames());

		if (env.getExecution().inStreamingMode()) {
			// determine gateway address (and port if possible)
			final InetAddress gatewayAddress = getGatewayAddress(env.getDeployment());
			final int gatewayPort = getGatewayPort(env.getDeployment());

			if (env.getExecution().isChangelogMode()) {
				return new ChangelogCollectStreamResult<>(
					outputType,
					schema,
					config,
					gatewayAddress,
					gatewayPort,
					classLoader);
			} else {
				throw new SqlExecutionException("Results of streaming queries can only be served in changelog mode.");
			}

		} else {
			// Batch Execution
			if (!env.getExecution().isTableMode()) {
				throw new SqlExecutionException("Results of batch queries can only be served in table mode.");
			}
			return new MaterializedCollectBatchResult<>(schema, outputType, config, classLoader);
		}
	}

	public void storeResult(String resultId, DynamicResult result) {
		results.put(resultId, result);
	}

	@SuppressWarnings("unchecked")
	public <T> DynamicResult<T> getResult(String resultId) {
		return (DynamicResult<T>) results.get(resultId);
	}

	public void removeResult(String resultId) {
		results.remove(resultId);
	}

	public List<String> getResults() {
		return new ArrayList<>(results.keySet());
	}

	// --------------------------------------------------------------------------------------------

	private int getGatewayPort(DeploymentEntry deploy) {
		// try to get address from deployment configuration
		return deploy.getGatewayPort();
	}

	private InetAddress getGatewayAddress(DeploymentEntry deploy) {
		// try to get address from deployment configuration
		final String address = deploy.getGatewayAddress();

		// use manually defined address
		if (!address.isEmpty()) {
			try {
				return InetAddress.getByName(address);
			} catch (UnknownHostException e) {
				throw new SqlGatewayException("Invalid gateway address '" + address + "' for result retrieval.", e);
			}
		} else {
			// TODO cache this
			// try to get the address by communicating to JobManager
			final String jobManagerAddress = flinkConfig.getString(JobManagerOptions.ADDRESS);
			final int jobManagerPort = flinkConfig.getInteger(JobManagerOptions.PORT);
			if (jobManagerAddress != null && !jobManagerAddress.isEmpty()) {
				try {
					return ConnectionUtils.findConnectingAddress(
						new InetSocketAddress(jobManagerAddress, jobManagerPort),
						deploy.getResponseTimeout(),
						400);
				} catch (Exception e) {
					throw new SqlGatewayException("Could not determine address of the gateway for result retrieval " +
						"by connecting to the job manager. Please specify the gateway address manually.", e);
				}
			} else {
				try {
					return InetAddress.getLocalHost();
				} catch (UnknownHostException e) {
					throw new SqlGatewayException("Could not determine address of the gateway for result retrieval. " +
						"Please specify the gateway address manually.", e);
				}
			}
		}
	}
}
