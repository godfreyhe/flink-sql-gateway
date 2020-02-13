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
 * Operation for CREATE VIEW command.
 */
public class CreateViewOperation implements NonJobOperation {
	private final String name;
	private final String query;
	private final String sessionId;
	private final Executor executor;

	public CreateViewOperation(String name, String query, String sessionId, Executor executor) {
		this.name = name;
		this.query = query;
		this.sessionId = sessionId;
		this.executor = executor;
	}

	@Override
	public ResultSet execute() {
		ViewEntry previousView = executor.listViews(sessionId).get(name);
		if (previousView != null) {
			throw new SqlExecutionException("'" + name + "' has already been defined in the current session.");
		}

		try {
			executor.addView(sessionId, name, query);
		} catch (SqlExecutionException e) {
			executor.removeView(sessionId, name);
			throw e;
		}
		return OperationUtil.AFFECTED_ROW_COUNT0;
	}
}
