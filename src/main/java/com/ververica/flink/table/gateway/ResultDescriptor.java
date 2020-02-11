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

import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.TableSchema;

import javax.annotation.Nullable;

import java.util.Optional;

/**
 * Describes a result to be expected from a table program.
 */
public class ResultDescriptor {

	private final String resultId;

	private final TableSchema resultSchema;

	private final boolean isMaterialized;

	// TODO refactor this
	@Nullable
	private final JobClient jobClient;

	public ResultDescriptor(String resultId, TableSchema resultSchema, boolean isMaterialized) {
		this(resultId, resultSchema, isMaterialized, null);
	}

	public ResultDescriptor(String resultId, TableSchema resultSchema, boolean isMaterialized,
		@Nullable JobClient jobClient) {
		this.resultId = resultId;
		this.resultSchema = resultSchema;
		this.isMaterialized = isMaterialized;
		this.jobClient = jobClient;
	}

	public String getResultId() {
		return resultId;
	}

	public TableSchema getResultSchema() {
		return resultSchema;
	}

	public boolean isMaterialized() {
		return isMaterialized;
	}

	public Optional<JobClient> getJobClient() {
		return Optional.ofNullable(jobClient);
	}
}
