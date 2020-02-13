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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Operation for SET command.
 */
public class SetOperation implements NonJobOperation {
	private final String key;
	private final String value;
	private final String sessionId;
	private final Executor executor;

	public SetOperation(String key, String value, String sessionId, Executor executor) {
		this.key = key;
		this.value = value;
		this.sessionId = sessionId;
		this.executor = executor;
	}

	@Override
	public ResultSet execute() {
		// list all properties
		if (key == null) {
			Map<String, String> properties = executor.getSessionProperties(sessionId);
			List<Row> data = new ArrayList<>(properties.size());
			int maxKeyLen = 1, maxValueLen = 1;
			for (Map.Entry<String, String> entry : properties.entrySet()) {
				String key = entry.getKey();
				String value = entry.getValue();
				data.add(Row.of(key, value));
				maxKeyLen = Math.max(maxKeyLen, key.length());
				maxValueLen = Math.max(maxValueLen, value.length());
			}
			return new ResultSet(
				Arrays.asList(
					ColumnInfo.create(ConstantNames.KEY, new VarCharType(true, maxKeyLen)),
					ColumnInfo.create(ConstantNames.VALUE, new VarCharType(true, maxValueLen))),
				data);
		} else {
			// set a property
			executor.setSessionProperty(sessionId, key, value);
			return OperationUtil.AFFECTED_ROW_COUNT0;
		}
	}
}
