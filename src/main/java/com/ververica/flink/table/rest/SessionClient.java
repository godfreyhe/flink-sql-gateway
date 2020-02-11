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

package com.ververica.flink.table.rest;

import com.ververica.flink.table.rest.handler.GetInfoHeaders;
import com.ververica.flink.table.rest.handler.JobCancelHeaders;
import com.ververica.flink.table.rest.handler.ResultFetchHeaders;
import com.ververica.flink.table.rest.handler.SessionCloseHeaders;
import com.ververica.flink.table.rest.handler.SessionCreateHeaders;
import com.ververica.flink.table.rest.handler.SessionHeartbeatHeaders;
import com.ververica.flink.table.rest.handler.StatementCompleteHeaders;
import com.ververica.flink.table.rest.handler.StatementExecuteHeaders;
import com.ververica.flink.table.rest.message.GetInfoResponseBody;
import com.ververica.flink.table.rest.message.ResultFetchMessageParameters;
import com.ververica.flink.table.rest.message.ResultFetchResponseBody;
import com.ververica.flink.table.rest.message.SessionCreateRequestBody;
import com.ververica.flink.table.rest.message.SessionJobMessageParameters;
import com.ververica.flink.table.rest.message.SessionMessageParameters;
import com.ververica.flink.table.rest.message.StatementCompleteRequestBody;
import com.ververica.flink.table.rest.message.StatementExecuteRequestBody;
import com.ververica.flink.table.rest.message.StatementExecuteResponseBody;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.RestClientConfiguration;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.types.Row;
import org.apache.flink.util.ExecutorUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * SessionClient.
 */
public class SessionClient {

	private static final Logger LOG = LoggerFactory.getLogger(SessionClient.class);

	private final String serverHost;
	private final int serverPort;
	private final String sessionName;
	private final String planner;
	private final String executionType;
	private final RestClient restClient;

	private final ExecutorService executor;
	private volatile String sessionId;
	private volatile boolean isClosed = false;

	public SessionClient(
		String serverHost,
		int serverPort,
		String sessionName,
		String planner,
		String executionType,
		String threadName)
		throws Exception {
		this.serverHost = serverHost;
		this.serverPort = serverPort;
		this.sessionName = sessionName;
		this.planner = planner;
		this.executionType = executionType;
		this.executor = Executors.newFixedThreadPool(4, new ExecutorThreadFactory(threadName));
		this.restClient = new RestClient(RestClientConfiguration.fromConfiguration(new Configuration()), executor);

		connectInternal();
	}

	public String getServerHost() {
		return serverHost;
	}

	public int getServerPort() {
		return serverPort;
	}

	public String getPlanner() {
		return planner;
	}

	public synchronized String getSessionId() {
		return sessionId;
	}

	public synchronized void reconnect() throws Exception {
		connectInternal();
	}

	private void connectInternal() throws Exception {
		this.sessionId = restClient.sendRequest(
			serverHost,
			serverPort,
			SessionCreateHeaders.getInstance(),
			EmptyMessageParameters.getInstance(),
			new SessionCreateRequestBody(sessionName, planner, executionType, Collections.emptyMap()))
			.get().getSessionId();
	}

	public synchronized void close() throws Exception {
		if (isClosed) {
			return;
		}
		isClosed = true;
		try {
			restClient.sendRequest(
				serverHost,
				serverPort,
				SessionCloseHeaders.getInstance(),
				new SessionMessageParameters(sessionId),
				EmptyRequestBody.getInstance()).get();
		} catch (Exception e) {
			// TODO print more message when connection refused or session not exists
			LOG.warn("Close session client failed.", e);
		} finally {
			restClient.shutdown(Time.seconds(5));
			ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, executor);
		}
	}

	public synchronized void sendHeartbeat() throws SqlRestException {
		checkState();
		try {
			restClient.sendRequest(
				serverHost,
				serverPort,
				SessionHeartbeatHeaders.getInstance(),
				new SessionMessageParameters(sessionId),
				EmptyRequestBody.getInstance())
				.get();
		} catch (Exception e) {
			throw new SqlRestException("Failed to send heartbeat to server", e);
		}
	}

	public StatementExecuteResponseBody submitStatement(String stmt) throws SqlRestException {
		return submitStatement(stmt, Long.MAX_VALUE);
	}

	public synchronized StatementExecuteResponseBody submitStatement(String stmt, long executionTimeoutMillis)
		throws SqlRestException {
		checkState();
		try {
			return restClient.sendRequest(
				serverHost,
				serverPort,
				StatementExecuteHeaders.getInstance(),
				new SessionMessageParameters(sessionId),
				new StatementExecuteRequestBody(stmt, executionTimeoutMillis))
				.get();
		} catch (Exception e) {
			throw new SqlRestException("Failed to submit statement", e);
		}
	}

	public synchronized List<String> completeStatement(String stmt, int position) throws SqlRestException {
		checkState();
		try {
			List<Row> rows = restClient.sendRequest(
				serverHost,
				serverPort,
				StatementCompleteHeaders.getInstance(),
				new SessionMessageParameters(sessionId),
				new StatementCompleteRequestBody(stmt, position))
				.get().getResult().getData();
			return singleStringColumn(rows);
		} catch (Exception e) {
			throw new SqlRestException("Failed to get completion hints", e);
		}
	}

	public synchronized void cancelJob(JobID jobId) throws SqlRestException {
		checkState();
		try {
			restClient.sendRequest(
				serverHost,
				serverPort,
				JobCancelHeaders.getInstance(),
				new SessionJobMessageParameters(sessionId, jobId),
				EmptyRequestBody.getInstance())
				.get();
		} catch (Exception e) {
			throw new SqlRestException("Failed to cancel job", e);
		}
	}

	public synchronized ResultFetchResponseBody fetchResult(JobID jobId, long token) throws SqlRestException {
		checkState();
		try {
			return restClient.sendRequest(
				serverHost,
				serverPort,
				ResultFetchHeaders.getInstance(),
				new ResultFetchMessageParameters(sessionId, jobId, token),
				EmptyRequestBody.getInstance())
				.get();
		} catch (Exception e) {
			throw new SqlRestException("Failed to fetch result", e.getCause());
		}
	}

	public GetInfoResponseBody getInfo() throws SqlRestException {
		checkState();
		try {
			return restClient.sendRequest(
				serverHost,
				serverPort,
				GetInfoHeaders.getInstance(),
				EmptyMessageParameters.getInstance(),
				EmptyRequestBody.getInstance())
				.get();
		} catch (Exception e) {
			throw new SqlRestException("Failed to get server info", e);
		}
	}

	private void checkState() {
		if (isClosed) {
			throw new IllegalStateException("Session is already closed.");
		}
	}

	public static List<String> singleStringColumn(List<Row> rows) {
		List<String> strings = new ArrayList<>(rows.size());
		for (Row row : rows) {
			strings.add(row.getField(0).toString());
		}
		return strings;
	}

}
