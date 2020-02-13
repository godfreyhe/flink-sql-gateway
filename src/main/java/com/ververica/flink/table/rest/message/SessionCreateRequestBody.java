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

package com.ververica.flink.table.rest.message;

import org.apache.flink.runtime.rest.messages.RequestBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * {@link RequestBody} for session create.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SessionCreateRequestBody implements RequestBody {

	private static final String FIELD_NAME_SESSION_NAME = "session_name";
	private static final String FIELD_NAME_PLANNER = "planner";
	private static final String FIELD_NAME_EXECUTION_TYPE = "execution_type";
	private static final String FIELD_NAME_PROPERTIES = "properties";

	@JsonProperty(FIELD_NAME_SESSION_NAME)
	@Nullable
	private final String sessionName;

	@JsonProperty(FIELD_NAME_PLANNER)
	@Nullable
	private final String planner;

	@JsonProperty(FIELD_NAME_EXECUTION_TYPE)
	@Nullable
	private final String executionType;

	@JsonProperty(FIELD_NAME_PROPERTIES)
	@Nullable
	private final Map<String, String> properties;

	public SessionCreateRequestBody(
		@JsonProperty(FIELD_NAME_SESSION_NAME) @Nullable String sessionName,
		@JsonProperty(FIELD_NAME_PLANNER) @Nullable String planner,
		@JsonProperty(FIELD_NAME_EXECUTION_TYPE) @Nullable String executionType,
		@JsonProperty(FIELD_NAME_PROPERTIES) @Nullable Map<String, String> properties) {
		this.sessionName = sessionName;
		this.planner = planner;
		this.executionType = executionType;
		this.properties = properties;
	}

	@Nullable
	@JsonIgnore
	public String getSessionName() {
		return sessionName;
	}

	@Nullable
	@JsonIgnore
	public String getPlanner() {
		return planner;
	}

	@Nullable
	@JsonIgnore
	public String getExecutionType() {
		return executionType;
	}

	@Nullable
	@JsonIgnore
	public Map<String, String> getProperties() {
		return properties;
	}

}
