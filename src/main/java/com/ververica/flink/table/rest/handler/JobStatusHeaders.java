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

import com.ververica.flink.table.rest.message.JobIdPathParameter;
import com.ververica.flink.table.rest.message.JobStatusResponseBody;
import com.ververica.flink.table.rest.message.SessionIdPathParameter;
import com.ververica.flink.table.rest.message.SessionJobMessageParameters;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.versioning.RestAPIVersion;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Headers for getting job status.
 */
public class JobStatusHeaders implements MessageHeaders<
	EmptyRequestBody, JobStatusResponseBody, SessionJobMessageParameters> {

	private static final JobStatusHeaders INSTANCE = new JobStatusHeaders();

	public static final String URL = "/sessions/:" + SessionIdPathParameter.KEY +
		"/jobs/:" + JobIdPathParameter.KEY + "/status";

	private JobStatusHeaders() {
	}

	@Override
	public Class<JobStatusResponseBody> getResponseClass() {
		return JobStatusResponseBody.class;
	}

	@Override
	public HttpResponseStatus getResponseStatusCode() {
		return HttpResponseStatus.OK;
	}

	@Override
	public String getDescription() {
		return "get job status.";
	}

	@Override
	public Class<EmptyRequestBody> getRequestClass() {
		return EmptyRequestBody.class;
	}

	@Override
	public SessionJobMessageParameters getUnresolvedMessageParameters() {
		return new SessionJobMessageParameters();
	}

	@Override
	public HttpMethodWrapper getHttpMethod() {
		return HttpMethodWrapper.GET;
	}

	@Override
	public String getTargetRestEndpointURL() {
		return URL;
	}

	public static JobStatusHeaders getInstance() {
		return INSTANCE;
	}

	public static String buildJobStatusUri(RestAPIVersion version, String sessionId, JobID jobId) {
		return "/" + version.getURLVersionPrefix() + URL
			.replace(":" + SessionIdPathParameter.KEY, sessionId)
			.replace(":" + JobIdPathParameter.KEY, jobId.toHexString());
	}

}
