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

import com.ververica.flink.table.config.Environment;
import com.ververica.flink.table.config.entries.ExecutionEntry;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * SessionManager.
 */
public class SessionManager {
	private final Environment defaultEnvironment;
	private final Executor executor;

	private final long idleTimeout;
	private final long checkInterval;
	private final long maxCount;

	private final Map<String, Session> sessions;

	private ScheduledFuture timeoutCheckerFuture;

	public SessionManager(Environment defaultEnvironment, Executor executor) {
		this.defaultEnvironment = defaultEnvironment;
		this.executor = executor;
		this.idleTimeout = defaultEnvironment.getSession().getIdleTimeout();
		this.checkInterval = defaultEnvironment.getSession().getCheckInterval();
		this.maxCount = defaultEnvironment.getSession().getMaxCount();
		this.sessions = new ConcurrentHashMap<>();
	}

	public void open() {
		executor.start();
		ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
		timeoutCheckerFuture = service.scheduleAtFixedRate(() -> {
			for (Map.Entry<String, Session> entry : sessions.entrySet()) {
				if (isSessionExpired(entry.getValue())) {
					closeSession(entry.getValue());
				}
			}
		}, checkInterval, checkInterval, TimeUnit.MILLISECONDS);
	}

	public void close() {
		timeoutCheckerFuture.cancel(true);
	}

	public String createSession(
		String sessionName,
		String planner,
		String executionType,
		Map<String, String> properties)
		throws SqlGatewayException {
		checkSessionCount();

		Map<String, String> newProperties = new HashMap<>(properties);
		newProperties.put(Environment.EXECUTION_ENTRY + "." + ExecutionEntry.EXECUTION_PLANNER, planner);
		newProperties.put(Environment.EXECUTION_ENTRY + "." + ExecutionEntry.EXECUTION_TYPE, executionType);

		if (executionType.equalsIgnoreCase(ExecutionEntry.EXECUTION_TYPE_VALUE_BATCH)) {
			// for batch mode we ensure that results are provided in materialized form
			newProperties.put(
				Environment.EXECUTION_ENTRY + "." + ExecutionEntry.EXECUTION_RESULT_MODE,
				ExecutionEntry.EXECUTION_RESULT_MODE_VALUE_TABLE);
		} else {
			// for streaming mode we ensure that results are provided in changelog form
			newProperties.put(
				Environment.EXECUTION_ENTRY + "." + ExecutionEntry.EXECUTION_RESULT_MODE,
				ExecutionEntry.EXECUTION_RESULT_MODE_VALUE_CHANGELOG);
		}

		Environment sessionEnv = Environment.enrich(defaultEnvironment, newProperties, Collections.emptyMap());

		String sessionId = SessionID.generate().toHexString();
		SessionContext ctx = new SessionContext(sessionName, sessionId, sessionEnv);
		Session session = new Session(ctx, executor);
		sessions.put(sessionId, session);

		return sessionId;
	}

	public void closeSession(String sessionId) throws SqlGatewayException {
		Session session = sessions.get(sessionId);
		if (session == null) {
			throw new SqlGatewayException("Session " + sessionId + " does not exist.");
		} else {
			closeSession(session);
		}
	}

	private void closeSession(Session session) {
		String sessionId = session.getSessionContext().getSessionId();
		session.close();
		sessions.remove(sessionId);
	}

	public Session getSession(String sessionId) throws SqlGatewayException {
		// TODO lock sessions to prevent fetching an expired session?
		Session session = sessions.get(sessionId);
		if (session == null) {
			throw new SqlGatewayException("Session " + sessionId + " does not exist.");
		}
		session.touch();
		return session;
	}

	private void checkSessionCount() throws SqlGatewayException {
		if (maxCount <= 0) {
			return;
		}
		if (sessions.size() > maxCount) {
			throw new SqlGatewayException("the count of alive session exceeds the max count: " + maxCount);
		}
	}

	private boolean isSessionExpired(Session session) {
		return (System.currentTimeMillis() - session.getLastVisitedTime()) > idleTimeout;
	}
}
