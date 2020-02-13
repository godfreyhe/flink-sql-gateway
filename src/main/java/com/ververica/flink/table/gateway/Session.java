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

import com.ververica.flink.table.client.cli.SqlCommandParser;
import com.ververica.flink.table.gateway.operation.JobOperation;
import com.ververica.flink.table.gateway.operation.Operation;
import com.ververica.flink.table.gateway.operation.OperationFactory;
import com.ververica.flink.table.gateway.operation.OperationUtil;
import com.ververica.flink.table.rest.result.ResultSet;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.java.tuple.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Session.
 */
public class Session {

	private static final Logger LOG = LoggerFactory.getLogger(Session.class);

	private final SessionContext ctx;
	private final String sessionId;
	private final Executor executor;

	private long lastVisitedTime;

	private final Map<JobID, JobOperation> jobOperations;

	public Session(SessionContext ctx, Executor executor) {
		this.ctx = ctx;
		this.sessionId = ctx.getSessionId();
		this.executor = executor;

		this.lastVisitedTime = System.currentTimeMillis();

		this.jobOperations = new ConcurrentHashMap<>();

		executor.openSession(ctx);
	}

	public void touch() {
		lastVisitedTime = System.currentTimeMillis();
	}

	public long getLastVisitedTime() {
		return lastVisitedTime;
	}

	public SessionContext getSessionContext() {
		return ctx;
	}

	public Tuple2<ResultSet, SqlCommandParser.SqlCommand> runStatement(String statement) throws SqlGatewayException {
		SqlCommandParser.SqlCommandCall call = SqlCommandParser.parse(statement).orElseThrow(
			() -> new SqlGatewayException("Unknown statement: " + statement));
		Operation operation = OperationFactory.createOperation(call, sessionId, executor);
		ResultSet resultSet = operation.execute();

		if (operation instanceof JobOperation) {
			JobOperation jobOperation = (JobOperation) operation;
			jobOperations.put(jobOperation.getJobId(), jobOperation);
		}

		return Tuple2.of(resultSet, call.command);
	}

	public ResultSet completeStatement(String statement, int position) throws SqlGatewayException {
		return OperationUtil.createCompleteStatementOperation(sessionId, statement, position, executor).execute();
	}

	public JobStatus getJobStatus(JobID jobId) throws SqlGatewayException {
		return getJobOperation(jobId).getJobStatus();
	}

	public void cancelJob(JobID jobId) throws SqlGatewayException {
		getJobOperation(jobId).cancelJob();
		jobOperations.remove(jobId);
	}

	public Optional<ResultSet> getJobResult(JobID jobId, long token, int fetchSize) throws SqlGatewayException {
		return getJobOperation(jobId).getJobResult(token, fetchSize);
	}

	private JobOperation getJobOperation(JobID jobId) throws SqlGatewayException {
		JobOperation jobOperation = jobOperations.get(jobId);
		if (jobOperation == null) {
			throw new SqlGatewayException("Job " + jobId + " does not exist in current session: " + sessionId + ".");
		} else {
			return jobOperation;
		}
	}

	public void close() {
		executor.closeSession(sessionId);
	}
}
