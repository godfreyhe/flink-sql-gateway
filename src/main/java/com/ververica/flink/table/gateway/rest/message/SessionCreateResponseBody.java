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

package com.ververica.flink.table.gateway.rest.message;

import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * ResponseBody for session create.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SessionCreateResponseBody implements ResponseBody {

	private static final String FIELD_SESSION_ID = "session_id";
	private static final String FIELD_NAME_SUBMIT_STATEMENT_URI = "submit_statement_uri";
	private static final String FIELD_NAME_CLOSE_SESSION = "close_session_uri";

	@JsonProperty(FIELD_SESSION_ID)
	private String sessionId;

	@JsonProperty(FIELD_NAME_SUBMIT_STATEMENT_URI)
	private final String submitStatementUri;

	@JsonProperty(FIELD_NAME_CLOSE_SESSION)
	private final String closeSessionUri;

	public SessionCreateResponseBody(
		@JsonProperty(FIELD_SESSION_ID) String sessionId,
		@JsonProperty(FIELD_NAME_SUBMIT_STATEMENT_URI) String submitStatementUri,
		@JsonProperty(FIELD_NAME_CLOSE_SESSION) String closeSessionUri) {

		this.sessionId = sessionId;
		this.submitStatementUri = submitStatementUri;
		this.closeSessionUri = closeSessionUri;
	}

	@JsonIgnore
	public String getSessionId() {
		return sessionId;
	}

	@JsonIgnore
	public String getSubmitStatementUri() {
		return submitStatementUri;
	}

	@JsonIgnore
	public String getCloseSessionUri() {
		return closeSessionUri;
	}
}
