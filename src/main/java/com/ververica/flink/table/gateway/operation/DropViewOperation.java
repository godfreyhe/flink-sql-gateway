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
import com.ververica.flink.table.gateway.SqlExecutionException;
import com.ververica.flink.table.gateway.config.entries.ViewEntry;
import com.ververica.flink.table.gateway.rest.result.ResultSet;

/**
 * Operation for DROP VIEW command.
 */
public class DropViewOperation implements NonJobOperation {
	private final String name;
	private final String sessionId;
	private final Executor executor;

	public DropViewOperation(String name, String sessionId, Executor executor) {
		this.name = name;
		this.sessionId = sessionId;
		this.executor = executor;
	}

	@Override
	public ResultSet execute() {
		ViewEntry view = executor.listViews(sessionId).get(name);
		if (view == null) {
			throw new SqlExecutionException("'" + name + "' does not exist in the current session.");
		}

		try {
			executor.removeView(sessionId, name);
		} catch (SqlExecutionException e) {
			executor.addView(sessionId, view.getName(), view.getQuery());
			throw e;
		}
		return OperationUtil.AFFECTED_ROW_COUNT0;
	}
}
