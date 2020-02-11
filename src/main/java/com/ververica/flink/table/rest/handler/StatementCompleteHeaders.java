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

package com.ververica.flink.table.rest.handler;

import com.ververica.flink.table.rest.message.SessionIdPathParameter;
import com.ververica.flink.table.rest.message.SessionMessageParameters;
import com.ververica.flink.table.rest.message.StatementCompleteRequestBody;
import com.ververica.flink.table.rest.message.StatementCompleteResponseBody;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.versioning.RestAPIVersion;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Headers for statement complete hints.
 */
public class StatementCompleteHeaders implements MessageHeaders<
	StatementCompleteRequestBody, StatementCompleteResponseBody, SessionMessageParameters> {

	private static final StatementCompleteHeaders INSTANCE = new StatementCompleteHeaders();

	public static final String URL = "/sessions/:" + SessionIdPathParameter.KEY + "/statements/complete_hints";

	private StatementCompleteHeaders() {
	}

	@Override
	public Class<StatementCompleteResponseBody> getResponseClass() {
		return StatementCompleteResponseBody.class;
	}

	@Override
	public HttpResponseStatus getResponseStatusCode() {
		return HttpResponseStatus.OK;
	}

	@Override
	public String getDescription() {
		return "get statement complete hints";
	}

	@Override
	public Class<StatementCompleteRequestBody> getRequestClass() {
		return StatementCompleteRequestBody.class;
	}

	@Override
	public SessionMessageParameters getUnresolvedMessageParameters() {
		return new SessionMessageParameters();
	}

	@Override
	public HttpMethodWrapper getHttpMethod() {
		return HttpMethodWrapper.POST;
	}

	@Override
	public String getTargetRestEndpointURL() {
		return URL;
	}

	public static StatementCompleteHeaders getInstance() {
		return INSTANCE;
	}

	public static String buildStatementCompleteUri(RestAPIVersion version, String sessionId) {
		return "/" + version.getURLVersionPrefix() +
			URL.replace(":" + SessionIdPathParameter.KEY, sessionId);
	}
}
