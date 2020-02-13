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
import com.ververica.flink.table.gateway.ProgramTargetDescriptor;
import com.ververica.flink.table.gateway.SqlGatewayException;
import com.ververica.flink.table.gateway.rest.result.ColumnInfo;
import com.ververica.flink.table.gateway.rest.result.ConstantNames;
import com.ververica.flink.table.gateway.rest.result.ResultSet;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;

import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Operation for INSERT command.
 */
public class InsertOperation extends AbstractJobOperation {
	private final String statement;
	private final String sessionId;
	private final Executor executor;

	private final List<ColumnInfo> columnInfos;

	private ProgramTargetDescriptor programTargetDescriptor;

	private boolean fetched = false;

	public InsertOperation(String statement, String sessionId, Executor executor) {
		this.statement = statement;
		this.sessionId = sessionId;
		this.executor = executor;

		this.columnInfos = new ArrayList<>();
		this.columnInfos.add(ColumnInfo.create(ConstantNames.AFFECTED_ROW_COUNT, new BigIntType(false)));
	}

	@Override
	public ResultSet execute() {
		programTargetDescriptor = executor.executeUpdate(sessionId, statement);
		jobId = programTargetDescriptor.getJobId();
		String strJobId = jobId.toString();
		return new ResultSet(
			Collections.singletonList(
				ColumnInfo.create(ConstantNames.JOB_ID, new VarCharType(false, strJobId.length()))),
			Collections.singletonList(Row.of(strJobId)));
	}

	@Override
	protected Optional<Tuple2<List<Row>, List<Boolean>>> fetchNewJobResults() throws SqlGatewayException {
		if (fetched) {
			return Optional.empty();
		} else {
			JobStatus jobStatus = getJobStatus();
			if (jobStatus.isGloballyTerminalState()) {
				// TODO get affected_row_count for batch job
				fetched = true;
				return Optional.of(Tuple2.of(Collections.singletonList(
					Row.of((long) Statement.SUCCESS_NO_INFO)), null));
			} else {
				// TODO throws exception if the job fails
				return Optional.of(Tuple2.of(Collections.emptyList(), null));
			}
		}
	}

	@Override
	protected List<ColumnInfo> getColumnInfos() {
		return columnInfos;
	}

	@Override
	public JobStatus getJobStatus() throws SqlGatewayException {
		if (jobId == null) {
			throw new IllegalStateException("No job has been submitted. This is a bug.");
		} else if (programTargetDescriptor == null) {
			// canceled by cancelJob method
			return JobStatus.CANCELED;
		}

		try {
			synchronized (lock) {
				return programTargetDescriptor.getJobClient().getJobStatus().get(30, TimeUnit.SECONDS);
			}
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			throw new SqlGatewayException("Failed to fetch job status for job " + jobId, e);
		}
	}

	@Override
	public void cancelJob() {
		if (programTargetDescriptor != null) {
			synchronized (lock) {
				if (programTargetDescriptor != null) {
					try {
						programTargetDescriptor.cancel();
					} finally {
						programTargetDescriptor = null;
					}
				}
			}
		}
	}
}
