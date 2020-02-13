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
import com.ververica.flink.table.gateway.rest.result.ColumnInfo;
import com.ververica.flink.table.gateway.rest.result.ConstantNames;
import com.ververica.flink.table.gateway.rest.result.ResultSet;

import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;

import java.util.Collections;

/**
 * Operation for EXPLAIN command.
 */
public class ExplainOperation implements NonJobOperation {
	private final String stmt;
	private final String sessionId;
	private final Executor executor;

	public ExplainOperation(String stmt, String sessionId, Executor executor) {
		this.stmt = stmt;
		this.sessionId = sessionId;
		this.executor = executor;
	}

	@Override
	public ResultSet execute() {
		String explanation = executor.explainStatement(sessionId, stmt);
		return new ResultSet(
			Collections.singletonList(
				ColumnInfo.create(ConstantNames.EXPLANATION, new VarCharType(false, explanation.length()))),
			Collections.singletonList(Row.of(explanation)));
	}
}
