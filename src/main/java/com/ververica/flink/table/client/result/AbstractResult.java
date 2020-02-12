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

package com.ververica.flink.table.client.result;

import com.ververica.flink.table.gateway.SqlExecutionException;
import com.ververica.flink.table.rest.SessionClient;
import com.ververica.flink.table.rest.SqlRestException;
import com.ververica.flink.table.rest.message.ResultFetchResponseBody;
import com.ververica.flink.table.rest.result.ColumnInfo;
import com.ververica.flink.table.rest.result.ResultSet;

import org.apache.flink.api.common.JobID;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.utils.LogicalTypeDataTypeConverter;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractResult implements Result {
	private final SessionClient sessionClient;
	private final JobID jobId;
	private final Object withTableSchemaReady = new Object();
	protected final Object resultLock = new Object();
	protected final AtomicReference<SqlExecutionException> executionException = new AtomicReference<>();
	private final ResultRetrievalThread retrievalThread;

	private TableSchema tableSchema;

	public AbstractResult(SessionClient sessionClient, JobID jobId) {
		this.sessionClient = sessionClient;
		this.jobId = jobId;
		this.retrievalThread = new ResultRetrievalThread();
		this.retrievalThread.start();
	}

	@Override
	public TableSchema getTableSchema() {
		if (tableSchema == null) {
			synchronized (withTableSchemaReady) {
				if (tableSchema == null) {
					try {
						withTableSchemaReady.wait();
					} catch (InterruptedException e) {
						throw new SqlExecutionException("get TableSchema interrupted", e);
					}
				}
			}
		}
		Preconditions.checkNotNull(tableSchema, "tableSchema is null");
		return tableSchema;
	}

	@Override
	public void close() {
		retrievalThread.isRunning = false;
		retrievalThread.interrupt();
		try {
			sessionClient.cancelJob(jobId);
		} catch (SqlRestException e) {
			throw new SqlExecutionException("Failed to close operation.", e);
		}
	}

	protected boolean isRetrieving() {
		return retrievalThread.isRunning;
	}

	protected abstract void processResult(ResultSet result);

	private class ResultRetrievalThread extends Thread {
		volatile boolean isRunning = true;
		private long token = -1;

		@Override
		public void run() {
			try {
				while (isRunning) {
					token += 1;
					ResultFetchResponseBody response = sessionClient.fetchResult(jobId, token);
					if (response.getNextResultUri() == null) {
						break;
					}

					int size = response.getResults().size();
					if (size != 1) {
						throw new SqlExecutionException(
							"Response should contain only one ResultSet, but " + size + " ResultSet(s) found.");
					}

					ResultSet result = response.getResults().get(0);
					if (tableSchema == null) {
						synchronized (withTableSchemaReady) {
							tableSchema = convertToTableSchema(result.getColumns());
							withTableSchemaReady.notify();
						}
					}

					if (result.getData() != null && !result.getData().isEmpty()) {
						processResult(result);
					} else {
						try {
							if (token >= 1) {
								Thread.sleep(100);
							}
						} catch (InterruptedException e) {
							throw new SqlExecutionException("Failed to fetch result", e);
						}
					}
				}
			} catch (SqlRestException e) {
				executionException.compareAndSet(
					null, new SqlExecutionException("Failed to retrieve results", e));
			} catch (RuntimeException e) {
				// ignore socket exceptions
			}

			// no result anymore
			// either the job is done or an error occurred
			isRunning = false;
		}
	}

	public TableSchema convertToTableSchema(List<ColumnInfo> columnInfos) {
		TableSchema.Builder builder = TableSchema.builder();
		for (ColumnInfo column : columnInfos) {
			builder.field(column.getName(), LogicalTypeDataTypeConverter.toDataType(column.getLogicalType()));
		}
		return builder.build();
	}
}
