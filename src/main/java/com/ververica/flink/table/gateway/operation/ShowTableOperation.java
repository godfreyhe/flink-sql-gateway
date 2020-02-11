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

import com.ververica.flink.table.config.entries.ViewEntry;
import com.ververica.flink.table.gateway.Executor;
import com.ververica.flink.table.rest.result.ColumnInfo;
import com.ververica.flink.table.rest.result.ConstantNames;
import com.ververica.flink.table.rest.result.ResultSet;

import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Operation for SHOW TABLE command.
 */
public class ShowTableOperation implements NonJobOperation {

	private final String sessionId;
	private final Executor executor;

	public ShowTableOperation(String sessionId, Executor executor) {
		this.sessionId = sessionId;
		this.executor = executor;
	}

	@Override
	public ResultSet execute() {
		List<Row> rows = new ArrayList<>();
		int maxNameLength = 1;
		List<String> views = new ArrayList<>();
		for (Map.Entry<String, ViewEntry> entry : executor.listViews(sessionId).entrySet()) {
			String name = entry.getKey();
			rows.add(Row.of(name, ConstantNames.VIEW_TYPE));
			maxNameLength = Math.max(maxNameLength, name.length());
			views.add(name);
		}

		// listTables will return all tables and views
		for (String table : executor.listTables(sessionId)) {
			if (!views.contains(table)) {
				rows.add(Row.of(table, ConstantNames.TABLE_TYPE));
				maxNameLength = Math.max(maxNameLength, table.length());
			}
		}

		return new ResultSet(
			Arrays.asList(
				ColumnInfo.create(ConstantNames.TABLES, new VarCharType(false, maxNameLength)),
				ColumnInfo.create(ConstantNames.TYPE, new VarCharType(
					false, Math.max(ConstantNames.VIEW_TYPE.length(), ConstantNames.TABLE_TYPE.length())))),
			rows);
	}
}
