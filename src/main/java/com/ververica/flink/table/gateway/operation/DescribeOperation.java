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
import com.ververica.flink.table.gateway.SqlGatewayException;
import com.ververica.flink.table.rest.result.ColumnInfo;
import com.ververica.flink.table.rest.result.ConstantNames;
import com.ververica.flink.table.rest.result.ResultSet;
import com.ververica.flink.table.rest.result.TableSchemaUtil;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Operation for DESCRIBE command.
 */
public class DescribeOperation implements NonJobOperation {

	private final String name;
	private final String sessionId;
	private final Executor executor;

	public DescribeOperation(String name, String sessionId, Executor executor) {
		this.name = name;
		this.sessionId = sessionId;
		this.executor = executor;
	}

	@Override
	public ResultSet execute() throws SqlGatewayException {
		TableSchema schema = executor.getTableSchema(sessionId, name);
		String schemaJson;
		try {
			schemaJson = TableSchemaUtil.writeTableSchemaToJson(schema);
		} catch (JsonProcessingException e) {
			throw new SqlGatewayException("Failed to serialize TableSchema to json", e);
		}

		int length = schemaJson.length();
		List<Row> data = new ArrayList<>();
		data.add(Row.of(schemaJson));

		return new ResultSet(
			Collections.singletonList(ColumnInfo.create(ConstantNames.SCHEMA, new VarCharType(true, length))),
			data);
	}
}
