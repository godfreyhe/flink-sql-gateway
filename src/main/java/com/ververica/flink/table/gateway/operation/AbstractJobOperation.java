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
import com.ververica.flink.table.gateway.rest.result.ColumnInfo;
import com.ververica.flink.table.gateway.rest.result.ResultSet;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

/**
 * A default implementation of JobOperation.
 */
public abstract class AbstractJobOperation implements JobOperation {
	protected volatile JobID jobId;

	private long currentToken;
	private int previousFetchSize;
	private int previousResultSetSize;
	private LinkedList<Row> bufferedResults;
	@Nullable
	private LinkedList<Boolean> bufferedChangeFlags;
	private boolean noMoreResults;

	protected final Object lock = new Object();

	public AbstractJobOperation() {
		this.currentToken = 0;
		this.previousFetchSize = 0;
		this.previousResultSetSize = 0;
		this.bufferedResults = new LinkedList<>();
		this.bufferedChangeFlags = null;
		this.noMoreResults = false;
	}

	@Override
	public JobID getJobId() {
		if (jobId == null) {
			throw new IllegalStateException("No job has been submitted. This is a bug.");
		}
		return jobId;
	}

	@Override
	public synchronized Optional<ResultSet> getJobResult(long token, int maxFetchSize) throws SqlGatewayException {
		if (token == currentToken) {
			if (noMoreResults) {
				return Optional.empty();
			}

			// a new token arrives, remove used results
			for (int i = 0; i < previousResultSetSize; i++) {
				bufferedResults.removeFirst();
				if (bufferedChangeFlags != null) {
					bufferedChangeFlags.removeFirst();
				}
			}

			if (bufferedResults.isEmpty()) {
				// buffered results have been totally consumed,
				// so try to fetch new results
				Optional<Tuple2<List<Row>, List<Boolean>>> newResults = fetchNewJobResults();
				if (newResults.isPresent()) {
					bufferedResults.addAll(newResults.get().f0);
					if (newResults.get().f1 != null) {
						if (bufferedChangeFlags == null) {
							bufferedChangeFlags = new LinkedList<>();
						}
						bufferedChangeFlags.addAll(newResults.get().f1);
					}
					currentToken++;
				} else {
					noMoreResults = true;
					return Optional.empty();
				}
			} else {
				// buffered results haven't been totally consumed
				currentToken++;
			}

			previousFetchSize = maxFetchSize;
			if (maxFetchSize > 0) {
				previousResultSetSize = Math.min(bufferedResults.size(), maxFetchSize);
			} else {
				previousResultSetSize = bufferedResults.size();
			}
		} else if (token == currentToken - 1 && token >= 0) {
			if (previousFetchSize != maxFetchSize) {
				throw new SqlGatewayException("As the same token is provided, fetch size must be the same.");
			}
		} else {
			String msg;
			if (currentToken == 0) {
				msg = "Expecting token to be 0, but found " + token + ".";
			} else {
				msg = "Expecting token to be " + currentToken + " or " + (currentToken - 1) + ", but found " + token + ".";
			}
			throw new SqlGatewayException(msg);
		}

		return Optional.of(new ResultSet(
			getColumnInfos(),
			getLinkedListElementsFromBegin(bufferedResults, previousResultSetSize),
			getLinkedListElementsFromBegin(bufferedChangeFlags, previousResultSetSize)));
	}

	protected abstract Optional<Tuple2<List<Row>, List<Boolean>>> fetchNewJobResults() throws SqlGatewayException;

	protected abstract List<ColumnInfo> getColumnInfos();

	private <T> List<T> getLinkedListElementsFromBegin(LinkedList<T> linkedList, int size) {
		if (linkedList == null) {
			return null;
		}
		List<T> ret = new ArrayList<>();
		Iterator<T> iter = linkedList.iterator();
		for (int i = 0; i < size; i++) {
			ret.add(iter.next());
		}
		return ret;
	}
}
