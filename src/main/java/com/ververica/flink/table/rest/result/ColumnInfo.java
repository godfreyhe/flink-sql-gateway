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

package com.ververica.flink.table.rest.result;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeParser;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/**
 * ColumnInfo.
 */
public class ColumnInfo {

	private static final String FIELD_NAME_NAME = "name";
	private static final String FIELD_NAME_TYPE = "type";

	@JsonProperty(FIELD_NAME_NAME)
	private String name;

	@JsonProperty(FIELD_NAME_TYPE)
	private String type;

	@JsonIgnore
	@Nullable
	private LogicalType logicalType;

	@JsonCreator
	public ColumnInfo(
		@JsonProperty(FIELD_NAME_NAME) String name,
		@JsonProperty(FIELD_NAME_TYPE) String type) {
		this.name = name;
		this.type = type;
	}

	// TODO delete this
	public ColumnInfo(String name, LogicalType logicalType) {
		this(name, logicalType.toString());
		this.logicalType = logicalType;
	}

	public static ColumnInfo create(String name, LogicalType type) {
		return new ColumnInfo(name, type.toString());
	}

	public String getName() {
		return name;
	}

	public String getType() {
		return type;
	}

	@JsonIgnore
	public LogicalType getLogicalType() {
		if (logicalType == null) {
			logicalType = LogicalTypeParser.parse(type);
		}
		return logicalType;
	}
}
