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
import com.ververica.flink.table.gateway.result.TypedResult;
import com.ververica.flink.table.rest.SessionClient;
import com.ververica.flink.table.rest.result.ResultSet;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * A result that works similarly to {@link DataStreamUtils#collect(DataStream)}.
 */
public abstract class StreamResult extends AbstractResult {

	public StreamResult(SessionClient sessionClient, JobID jobId) {
		super(sessionClient, jobId);
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

	@Override
	protected void processResult(ResultSet result) {
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

	protected abstract void processRecord(Tuple2<Boolean, Row> change);

}
