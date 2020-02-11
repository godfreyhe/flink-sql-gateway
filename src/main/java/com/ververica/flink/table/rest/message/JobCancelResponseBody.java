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

import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * ResponseBody for operation close.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class JobCancelResponseBody implements ResponseBody {

	private static final String FIELD_NAME_STATUS = "status";
	private static final String FIELD_NAME_SUBMIT_STATEMENT_URI = "submit_statement_uri";
	private static final String FIELD_NAME_CLOSE_SESSION = "close_session_uri";

	@JsonProperty(FIELD_NAME_STATUS)
	private String status;

	@JsonProperty(FIELD_NAME_SUBMIT_STATEMENT_URI)
	private final String submitStatementUri;

	@JsonProperty(FIELD_NAME_CLOSE_SESSION)
	private final String closeSessionUri;

	public JobCancelResponseBody(
		@JsonProperty(FIELD_NAME_STATUS) String status,
		@JsonProperty(FIELD_NAME_SUBMIT_STATEMENT_URI) String submitStatementUri,
		@JsonProperty(FIELD_NAME_CLOSE_SESSION) String closeSessionUri) {
		this.status = status;
		this.submitStatementUri = submitStatementUri;
		this.closeSessionUri = closeSessionUri;
	}

	public String getStatus() {
		return status;
	}

	public String getSubmitStatementUri() {
		return submitStatementUri;
	}

	public String getCloseSessionUri() {
		return closeSessionUri;
	}
}
