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

import com.ververica.flink.table.gateway.SqlGatewayException;
import com.ververica.flink.table.rest.result.ResultSet;

import org.apache.flink.api.common.JobID;

import java.util.Optional;

/**
 * Abstract Operation.
 */
public abstract class AbstractJobOperation implements JobOperation {

	protected volatile JobID jobId;

	private long currentToken;
	private ResultSet previousResultSet;

	protected final Object lock = new Object();

	public AbstractJobOperation() {
		this.currentToken = 0;
	}

	@Override
	public JobID getJobId() {
		if (jobId == null) {
			throw new IllegalStateException("No job has been submitted. This is a bug.");
		}
		return jobId;
	}

	@Override
	public Optional<ResultSet> getJobResult(long token) throws SqlGatewayException {
		if (token == currentToken) {
			Optional<ResultSet> currentResultSet = fetchNewJobResultSet();
			if (currentResultSet.isPresent()) {
				previousResultSet = currentResultSet.get();
				currentToken++;
			}
			return currentResultSet;
		} else if (token == currentToken - 1 && token >= 0) {
			return Optional.of(previousResultSet);
		} else {
			throw new SqlGatewayException(
				"Expecting token to be " + currentToken + " or " + (currentToken - 1) + ", but found " + token + ".");
		}
	}

	protected abstract Optional<ResultSet> fetchNewJobResultSet() throws SqlGatewayException;
}
