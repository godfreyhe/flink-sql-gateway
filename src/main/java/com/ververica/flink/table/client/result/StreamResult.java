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

import com.ververica.flink.table.client.SqlClientException;
import com.ververica.flink.table.gateway.SqlExecutionException;
import com.ververica.flink.table.gateway.result.TypedResult;
import com.ververica.flink.table.rest.SessionClient;
import com.ververica.flink.table.rest.SqlRestException;
import com.ververica.flink.table.rest.message.ResultFetchResponseBody;
import com.ververica.flink.table.rest.result.ResultSet;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A result that works similarly to {@link DataStreamUtils#collect(DataStream)}.
 */
public abstract class StreamResult implements Result {

	private final ResultRetrievalThread retrievalThread;

	private final SessionClient session;
	private final JobID jobId;
	protected final Object resultLock;
	private final Object withTableSchemaReady;

	private TableSchema tableSchema;

	protected AtomicReference<SqlExecutionException> executionException = new AtomicReference<>();

	public StreamResult(SessionClient session, JobID jobId) {

		this.session = session;
		this.jobId = jobId;
		this.resultLock = new Object();
		this.withTableSchemaReady = new Object();
		retrievalThread = new ResultRetrievalThread();
		retrievalThread.start();
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
			session.cancelJob(jobId);
		} catch (SqlRestException e) {
			throw new SqlExecutionException("close operation failed.", e);
		}
	}

	protected <T> TypedResult<T> handleMissingResult() {
		// check if the monitoring thread is still there
		// we need to wait until we know what is going on
		if (isRetrieving()) {
			return TypedResult.empty();
		}

		if (executionException.get() != null) {
			throw executionException.get();
		}

		// we assume that a bounded job finished
		return TypedResult.endOfStream();
	}

	// --------------------------------------------------------------------------------------------

	protected boolean isRetrieving() {
		return retrievalThread.isRunning;
	}

	protected abstract void processRecord(Tuple2<Boolean, Row> change);

	// --------------------------------------------------------------------------------------------

	private class ResultRetrievalThread extends Thread {

		public volatile boolean isRunning = true;
		private long token = 0;

		@Override
		public void run() {
			try {
				while (isRunning) {
					ResultFetchResponseBody response = session.fetchResult(jobId, token);
					int size = response.getResults().size();
					if (size != 1) {
						throw new SqlExecutionException(
							"result should contain only one ResultSet, but " + size + " ResultSet(s) found.");
					}

					ResultSet result = response.getResults().get(0);
					if (tableSchema == null) {
						synchronized (withTableSchemaReady) {
							tableSchema = ResultUtil.convertToTableSchema(result.getColumns());
							withTableSchemaReady.notify();
						}
					}

					if (response.getNextResultUri() == null) {
						break;
					}

					List<Row> data = result.getData();
					if (data != null && data.size() > 0) {
						if (!result.getChangeFlags().isPresent()) {
							throw new SqlClientException(
								"changeFlag is required, while result doesn't contain any changeFlag");
						}
						List<Boolean> changeFlags = result.getChangeFlags().get();
						if (changeFlags.size() != data.size()) {
							throw new SqlClientException("The size of data and changeFlags is different");
						}
						for (int i = 0; i < data.size(); ++i) {
							processRecord(new Tuple2<>(changeFlags.get(i), data.get(i)));
						}
					}
				}
			} catch (SqlRestException e) {
				executionException.compareAndSet(
					null, new SqlExecutionException("Failed to retrieve streaming results", e));
			} catch (RuntimeException e) {
				// ignore socket exceptions
			}

			// no result anymore
			// either the job is done or an error occurred
			isRunning = false;
		}
	}

}
