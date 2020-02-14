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

import com.ververica.flink.table.gateway.ProgramDeployer;
import com.ververica.flink.table.gateway.ProgramTargetDescriptor;
import com.ververica.flink.table.gateway.SqlExecutionException;
import com.ververica.flink.table.gateway.SqlGatewayException;
import com.ververica.flink.table.gateway.context.ExecutionContext;
import com.ververica.flink.table.gateway.context.SessionContext;
import com.ververica.flink.table.gateway.rest.result.ColumnInfo;
import com.ververica.flink.table.gateway.rest.result.ConstantNames;
import com.ververica.flink.table.gateway.rest.result.ResultSet;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.StreamQueryConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
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
import java.util.regex.Pattern;

/**
 * Operation for INSERT command.
 */
public class InsertOperation extends AbstractJobOperation {
	private final SessionContext context;
	private final String statement;
	// insert into sql match pattern
	private static final Pattern INSERT_SQL_PATTERN = Pattern.compile("(INSERT\\s+(INTO|OVERWRITE).*)",
		Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

	private final List<ColumnInfo> columnInfos;

	private ProgramTargetDescriptor programTargetDescriptor;

	private boolean fetched = false;

	public InsertOperation(SessionContext context, String statement) {
		this.context = context;
		this.statement = statement;

		this.columnInfos = new ArrayList<>();
		this.columnInfos.add(ColumnInfo.create(ConstantNames.AFFECTED_ROW_COUNT, new BigIntType(false)));
	}

	@Override
	public ResultSet execute() {
		programTargetDescriptor = executeUpdateInternal(context.getExecutionContext());
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

	private <C> ProgramTargetDescriptor executeUpdateInternal(ExecutionContext<C> executionContext) {
		TableEnvironment tableEnv = executionContext.getTableEnvironment();
		// parse and validate statement
		try {
			executionContext.wrapClassLoader(() -> {
				if (tableEnv instanceof StreamTableEnvironment) {
					((StreamTableEnvironment) tableEnv)
						.sqlUpdate(statement, (StreamQueryConfig) executionContext.getQueryConfig());
				} else {
					tableEnv.sqlUpdate(statement);
				}
				return null;
			});
		} catch (Throwable t) {
			// catch everything such that the statement does not crash the executor
			throw new SqlExecutionException("Invalid SQL update statement.", t);
		}

		//Todo: we should refactor following condition after TableEnvironment has support submit job directly.
		if (!INSERT_SQL_PATTERN.matcher(statement.trim()).matches()) {
			throw new SqlExecutionException("Only insert is supported now");
		}

		Optional<String> sessionName = context.getSessionName();
		String sessionId = context.getSessionId();
		String jobName = sessionId + ": " + statement;
		if (sessionName.isPresent()) {
			jobName = sessionName.get() + ": " + jobName;
		}

		// create job graph with dependencies
		final Pipeline pipeline;
		try {
			pipeline = executionContext.createPipeline(jobName, executionContext.getFlinkConfig());
		} catch (Throwable t) {
			// catch everything such that the statement does not crash the executor
			throw new SqlExecutionException("Invalid SQL statement.", t);
		}

		// create a copy so that we can change settings without affecting the original config
		Configuration configuration = new Configuration(executionContext.getFlinkConfig());
		// for update queries we don't wait for the job result, so run in detached mode
		configuration.set(DeploymentOptions.ATTACHED, false);

		// create execution
		final ProgramDeployer deployer = new ProgramDeployer(configuration, jobName, pipeline);

		// blocking deployment
		try {
			JobClient jobClient = deployer.deploy().get();
			return ProgramTargetDescriptor.of(jobClient);
		} catch (Exception e) {
			throw new RuntimeException("Error running SQL job.", e);
		}
	}
}
