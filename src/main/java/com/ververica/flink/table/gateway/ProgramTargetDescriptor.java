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

package com.ververica.flink.table.gateway;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.execution.JobClient;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Describes the target where a table program has been submitted to.
 */
public class ProgramTargetDescriptor {

	private final JobID jobId;
	private JobClient jobClient;

	public ProgramTargetDescriptor(JobID jobId) {
		this.jobId = checkNotNull(jobId);
	}

	public ProgramTargetDescriptor(JobClient jobClient) {
		this.jobId = checkNotNull(jobClient.getJobID());
		this.jobClient = jobClient;
	}

	public JobID getJobId() {
		return jobId;
	}

	public JobClient getJobClient() {
		return jobClient;
	}

	public void cancel() {
		if (jobClient != null) {
			jobClient.cancel();
		}
	}

	@Override
	public String toString() {
		return String.format("Job ID: %s\n", jobId);
	}

	/**
	 * Creates a program target description from deployment classes.
	 *
	 * @param jobId job id
	 * @return program target descriptor
	 */
	public static ProgramTargetDescriptor of(JobID jobId) {
		return new ProgramTargetDescriptor(jobId);
	}

	public static ProgramTargetDescriptor of(JobClient jobClient) {
		return new ProgramTargetDescriptor(jobClient);
	}
}
