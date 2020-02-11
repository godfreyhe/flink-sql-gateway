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

package com.ververica.flink.table.gateway.operation;

import com.ververica.flink.table.gateway.Executor;
import com.ververica.flink.table.gateway.ResultDescriptor;
import com.ververica.flink.table.gateway.SqlGatewayException;
import com.ververica.flink.table.gateway.result.TypedResult;
import com.ververica.flink.table.rest.result.ColumnInfo;
import com.ververica.flink.table.rest.result.ConstantNames;
import com.ververica.flink.table.rest.result.ResultSet;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Operation for SELECT command.
 */
public class SelectOperation extends AbstractJobOperation {

	private final String sql;
	private final String sessionId;
	private final Executor executor;

	private volatile ResultDescriptor resultDescriptor;

	private boolean isBatch;
	private List<ColumnInfo> columnInfos;

	private boolean resultFetched;

	public SelectOperation(String sql, String sessionId, Executor executor) {
		this.sql = sql;
		this.sessionId = sessionId;
		this.executor = executor;

		this.resultFetched = false;
	}

	@Override
	public ResultSet execute() throws SqlGatewayException {
		resultDescriptor = executor.executeQuery(sessionId, sql);
		isBatch = resultDescriptor.isMaterialized();

		List<TableColumn> resultSchemaColumns = resultDescriptor.getResultSchema().getTableColumns();
		columnInfos = new ArrayList<>();
		for (TableColumn column : resultSchemaColumns) {
			columnInfos.add(ColumnInfo.create(column.getName(), column.getType().getLogicalType()));
		}

		if (resultDescriptor.getJobClient().isPresent()) {
			JobClient jobClient = resultDescriptor.getJobClient().get();
			jobId = jobClient.getJobID();
		} else {
			throw new SqlGatewayException("Failed to submit job.");
		}

		return new ResultSet(
			Collections.singletonList(
				ColumnInfo.create(ConstantNames.JOB_ID, new VarCharType(false, jobId.toString().length()))),
			Collections.singletonList(Row.of(jobId.toString())));
	}

	@Override
	public JobStatus getJobStatus() throws SqlGatewayException {
		if (jobId == null) {
			throw new IllegalStateException("No job has been submitted. This is a bug.");
		} else if (resultDescriptor == null) {
			// canceled by cancelJob method
			return JobStatus.CANCELED;
		}

		synchronized (lock) {
			if (resultDescriptor.getJobClient().isPresent()) {
				try {
					return resultDescriptor.getJobClient().get()
						.getJobStatus().get(30, TimeUnit.SECONDS);
				} catch (InterruptedException | ExecutionException | TimeoutException e) {
					throw new SqlGatewayException("Failed to fetch job status for job " + jobId, e);
				}
			} else {
				throw new SqlGatewayException("Failed to fetch job status for job " + jobId);
			}
		}
	}

	@Override
	public void cancelJob() {
		if (resultDescriptor != null) {
			synchronized (lock) {
				if (resultDescriptor != null) {
					try {
						executor.cancelQuery(sessionId, resultDescriptor.getResultId());
					} finally {
						resultDescriptor = null;
						jobId = null;
					}
				}
			}
		}
	}

	@Override
	protected Optional<ResultSet> fetchNewJobResultSet() throws SqlGatewayException {
		if (resultDescriptor == null) {
			throw new SqlGatewayException("The job for this query has been canceled.");
		}

		Optional<ResultSet> ret;
		synchronized (lock) {
			if (resultDescriptor == null) {
				throw new SqlGatewayException("The job for this query has been canceled.");
			}

			if (isBatch) {
				ret = fetchBatchResult();
			} else {
				ret = fetchStreamingResult();
			}
		}
		resultFetched = true;
		return ret;
	}

	private Optional<ResultSet> fetchBatchResult() {
		String resultId = resultDescriptor.getResultId();
		TypedResult<Integer> typedResult = executor.snapshotResult(sessionId, resultId, Integer.MAX_VALUE);
		if (typedResult.getType() == TypedResult.ResultType.EOS) {
			if (resultFetched) {
				return Optional.empty();
			} else {
				return Optional.of(new ResultSet(columnInfos, new ArrayList<>(), null));
			}
		} else if (typedResult.getType() == TypedResult.ResultType.PAYLOAD) {
			Integer payload = typedResult.getPayload();
			List<Row> data = new ArrayList<>();
			for (int i = 1; i <= payload; i++) {
				data.addAll(executor.retrieveResultPage(resultId, i));
			}
			return Optional.of(new ResultSet(columnInfos, data, null));
		} else {
			return Optional.of(new ResultSet(columnInfos, new ArrayList<>(), null));
		}
	}

	private Optional<ResultSet> fetchStreamingResult() {
		TypedResult<List<Tuple2<Boolean, Row>>> typedResult = executor.retrieveResultChanges(
			sessionId, resultDescriptor.getResultId());
		if (typedResult.getType() == TypedResult.ResultType.EOS) {
			// According to the implementation of ChangelogCollectStreamResult,
			// if a streaming job producing no result finished and no attempt has been made to fetch the result,
			// EOS will be returned.
			// In order to deliver column info to the user, we have to return at least one empty result.
			if (resultFetched) {
				return Optional.empty();
			} else {
				return Optional.of(new ResultSet(columnInfos, new ArrayList<>(), new ArrayList<>()));
			}
		} else if (typedResult.getType() == TypedResult.ResultType.PAYLOAD) {
			List<Tuple2<Boolean, Row>> payload = typedResult.getPayload();
			List<Row> data = new ArrayList<>();
			List<Boolean> changeFlags = new ArrayList<>();
			for (Tuple2<Boolean, Row> tuple : payload) {
				data.add(tuple.f1);
				changeFlags.add(tuple.f0);
			}
			return Optional.of(new ResultSet(columnInfos, data, changeFlags));
		} else {
			return Optional.of(new ResultSet(columnInfos, new ArrayList<>(), new ArrayList<>()));
		}
	}

}
