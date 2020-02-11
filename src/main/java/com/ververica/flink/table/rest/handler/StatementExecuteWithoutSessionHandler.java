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

package com.ververica.flink.table.rest.handler;

import com.ververica.flink.table.config.entries.ExecutionEntry;
import com.ververica.flink.table.gateway.SessionManager;
import com.ververica.flink.table.gateway.SqlGatewayException;
import com.ververica.flink.table.rest.message.StatementExecuteWithoutSessionRequestBody;
import com.ververica.flink.table.rest.message.StatementExecuteWithoutSessionResponseBody;
import com.ververica.flink.table.rest.result.ColumnInfo;
import com.ververica.flink.table.rest.result.ResultSet;
import com.ververica.flink.table.rest.result.ResultSetUtil;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.types.Either;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Handler for statement execute without session.
 * This handler is currently only for testing.
 */
public class StatementExecuteWithoutSessionHandler extends AbstractRestHandler<
	StatementExecuteWithoutSessionRequestBody, StatementExecuteWithoutSessionResponseBody, EmptyMessageParameters> {

	private static final List<String> AVAILABLE_PLANNERS = Arrays.asList(
		ExecutionEntry.EXECUTION_PLANNER_VALUE_OLD,
		ExecutionEntry.EXECUTION_PLANNER_VALUE_BLINK);

	private static final List<String> AVAILABLE_EXECUTION_TYPES = Arrays.asList(
		ExecutionEntry.EXECUTION_TYPE_VALUE_BATCH,
		ExecutionEntry.EXECUTION_TYPE_VALUE_STREAMING);

	private final SessionManager sessionManager;

	public StatementExecuteWithoutSessionHandler(
		SessionManager sessionManager,
		Time timeout,
		Map<String, String> responseHeaders,
		MessageHeaders<
			StatementExecuteWithoutSessionRequestBody,
			StatementExecuteWithoutSessionResponseBody,
			EmptyMessageParameters> messageHeaders) {

		super(timeout, responseHeaders, messageHeaders);
		this.sessionManager = sessionManager;
	}

	@Override
	protected CompletableFuture<StatementExecuteWithoutSessionResponseBody> handleRequest(
		@Nonnull HandlerRequest<StatementExecuteWithoutSessionRequestBody, EmptyMessageParameters> request)
		throws RestHandlerException {

		String planner = request.getRequestBody().getPlanner();
		if (planner == null) {
			throw new RestHandlerException("Planner must be provided.", HttpResponseStatus.BAD_REQUEST);
		} else if (!AVAILABLE_PLANNERS.contains(planner)) {
			throw new RestHandlerException(
				"Planner must be one of these: " + String.join(", ", AVAILABLE_PLANNERS),
				HttpResponseStatus.BAD_REQUEST);
		}

		String executionType = request.getRequestBody().getExecutionType();
		if (executionType == null) {
			throw new RestHandlerException("Execution type must be provided.", HttpResponseStatus.BAD_REQUEST);
		} else if (!AVAILABLE_EXECUTION_TYPES.contains(executionType)) {
			throw new RestHandlerException(
				"Execution type must be one of these: " + String.join(", ", AVAILABLE_EXECUTION_TYPES),
				HttpResponseStatus.BAD_REQUEST);
		}

		Map<String, String> properties = request.getRequestBody().getProperties();
		if (properties == null) {
			properties = Collections.emptyMap();
		}

		String statement = request.getRequestBody().getStatement();
		if (statement == null) {
			throw new RestHandlerException("Statement must be provided.", HttpResponseStatus.BAD_REQUEST);
		}

		String sessionId = null;
		try {
			sessionId = sessionManager.createSession(null, planner, executionType, properties);
			ResultSet jobIdResultSet = sessionManager.getSession(sessionId).runStatement(statement).f0;
			Either<JobID, ResultSet> jobIdOrResultSet = ResultSetUtil.getEitherJobIdOrResultSet(jobIdResultSet);
			if (jobIdOrResultSet.isLeft()) {
				ResultSet realResultSet = getResult(sessionId, jobIdOrResultSet.left());
				return CompletableFuture.completedFuture(new StatementExecuteWithoutSessionResponseBody(realResultSet));
			} else {
				throw new RestHandlerException(
					"Statement must be either SELECT or INSERT", HttpResponseStatus.BAD_REQUEST);
			}
		} catch (InterruptedException | SqlGatewayException e) {
			throw new RestHandlerException(e.getMessage(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
		} finally {
			if (sessionId != null) {
				try {
					sessionManager.closeSession(sessionId);
				} catch (SqlGatewayException e) {
				}
			}
		}
	}

	private ResultSet getResult(String sessionId, JobID jobId) throws InterruptedException, SqlGatewayException {
		List<ResultSet> results = new ArrayList<>();
		long token = 0;

		while (true) {
			Optional<ResultSet> resultSet = sessionManager.getSession(sessionId).getJobResult(jobId, token);
			if (resultSet.isPresent()) {
				if (resultSet.get().getData().isEmpty()) {
					if (results.isEmpty()) {
						results.add(resultSet.get());
					}
					if (token >= 1) {
						Thread.sleep(500);
					}
				} else {
					results.add(resultSet.get());
				}
				token += 1;
			} else {
				break;
			}
		}

		if (results.isEmpty()) {
			return null;
		} else if (results.size() == 1) {
			return results.get(0);
		} else {
			ResultSet first = results.get(0);
			List<ColumnInfo> columns = new ArrayList<>(first.getColumns());
			List<Row> data = new ArrayList<>(first.getData());
			List<Boolean> changeFlags;
			boolean hasChangeFlag = first.getChangeFlags().isPresent();
			if (hasChangeFlag) {
				changeFlags = new ArrayList<>(first.getChangeFlags().get());
			} else {
				changeFlags = null;
			}
			for (int i = 1; i < results.size(); ++i) {
				ResultSet resultSet = results.get(i);
				data.addAll(resultSet.getData());
				if (hasChangeFlag) {
					Preconditions.checkArgument(resultSet.getChangeFlags().isPresent());
					changeFlags.addAll(resultSet.getChangeFlags().get());
				}
			}
			return new ResultSet(columns, data, changeFlags);
		}
	}
}
