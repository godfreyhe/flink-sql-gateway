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

import com.ververica.flink.table.rest.message.JobCancelResponseBody;
import com.ververica.flink.table.rest.message.JobIdPathParameter;
import com.ververica.flink.table.rest.message.SessionIdPathParameter;
import com.ververica.flink.table.rest.message.SessionJobMessageParameters;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.versioning.RestAPIVersion;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Headers for canceling a job.
 */
public class JobCancelHeaders implements MessageHeaders<
	EmptyRequestBody, JobCancelResponseBody, SessionJobMessageParameters> {

	private static final JobCancelHeaders INSTANCE = new JobCancelHeaders();

	public static final String URL = "/sessions/:" + SessionIdPathParameter.KEY +
		"/jobs/:" + JobIdPathParameter.KEY;

	private JobCancelHeaders() {
	}

	@Override
	public Class<JobCancelResponseBody> getResponseClass() {
		return JobCancelResponseBody.class;
	}

	@Override
	public HttpResponseStatus getResponseStatusCode() {
		return HttpResponseStatus.OK;
	}

	@Override
	public String getDescription() {
		return "Cancels a running job.";
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
		return HttpMethodWrapper.DELETE;
	}

	@Override
	public String getTargetRestEndpointURL() {
		return URL;
	}

	public static JobCancelHeaders getInstance() {
		return INSTANCE;
	}

	public static String buildCancelJobUri(RestAPIVersion version, String sessionId, JobID jobId) {
		return "/" + version.getURLVersionPrefix() + URL
			.replace(":" + SessionIdPathParameter.KEY, sessionId)
			.replace(":" + JobIdPathParameter.KEY, jobId.toHexString());
	}

}
