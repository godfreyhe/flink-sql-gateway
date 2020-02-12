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
import com.ververica.flink.table.gateway.result.TypedResult;
import com.ververica.flink.table.rest.SessionClient;
import com.ververica.flink.table.rest.result.ResultSet;

import org.apache.flink.api.common.JobID;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * Collects results using accumulators and returns them as table snapshots.
 */
public class MaterializedBatchResult extends AbstractResult implements MaterializedResult {
	private int pageSize;
	private int pageCount;
	private List<Row> resultTable;

	private volatile boolean snapshotted = false;

	public MaterializedBatchResult(SessionClient sessionClient, JobID jobId) {
		super(sessionClient, jobId);
		this.pageCount = 0;
		this.resultTable = new ArrayList<>();
	}

	@Override
	public boolean isMaterialized() {
		return true;
	}

	@Override
	public List<Row> retrievePage(int page) {
		synchronized (resultLock) {
			if (page <= 0 || page > pageCount) {
				throw new SqlExecutionException("Invalid page '" + page + "'.");
			}
			return resultTable.subList(pageSize * (page - 1), Math.min(resultTable.size(), page * pageSize));
		}
	}

	@Override
	public TypedResult<Integer> snapshot(int pageSize) {
		synchronized (resultLock) {
			// the job finished with an exception
			SqlExecutionException e = executionException.get();
			if (e != null) {
				throw e;
			}

			// wait for a result
			if (null == resultTable) {
				return TypedResult.empty();
			}
			// we return a payload result the first time and EoS for the rest of times as if the results
			// are retrieved dynamically
			else if (!snapshotted) {
				snapshotted = true;
				this.pageSize = pageSize;
				pageCount = Math.max(1, (int) Math.ceil(((double) resultTable.size() / pageSize)));
				return TypedResult.payload(pageCount);
			} else {
				return TypedResult.endOfStream();
			}
		}
	}

	@Override
	protected void processResult(ResultSet result) {
		List<Row> data = result.getData();
		if (data != null && data.size() > 0) {
			resultTable.addAll(data);
		}
	}

}
