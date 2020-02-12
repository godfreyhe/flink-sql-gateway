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

import com.ververica.flink.table.client.cli.SqlCommandParser;
import com.ververica.flink.table.gateway.Executor;

/**
 * Util class.
 */
public class OperationFactory {

	public static Operation createOperation(
		SqlCommandParser.SqlCommandCall call,
		String sessionId,
		Executor executor) {

		Operation operation;
		switch (call.command) {
			case SELECT:
				operation = new SelectOperation(call.operands[0], sessionId, executor);
				break;
			case CREATE_TABLE:
				operation = new CreateTableOperation(call.operands[0], sessionId, executor);
				break;
			case DROP_TABLE:
				operation = new DropTableOperation(call.operands[0], sessionId, executor);
				break;
			case CREATE_VIEW:
				operation = new CreateViewOperation(
					call.operands[0], call.operands[1], sessionId, executor);
				break;
			case DROP_VIEW:
				operation = new DropViewOperation(call.operands[0], sessionId, executor);
				break;
			case CREATE_DATABASE:
			case DROP_DATABASE:
			case ALTER_DATABASE:
			case ALTER_TABLE:
				operation = new UpdateOperation(call.operands[0], sessionId, executor);
				break;
			case SET:
				// list all properties
				if (call.operands.length == 0) {
					operation = new SetOperation(null, null, sessionId, executor);
				} else {
					// set a property
					operation = new SetOperation(call.operands[0], call.operands[1], sessionId, executor);
				}
				break;
			case RESET:
				operation = new ResetOperation(sessionId, executor);
				break;
			case USE_CATALOG:
				operation = new UseCatalogOperation(call.operands[0], sessionId, executor);
				break;
			case USE:
				operation = new UseDatabaseOperation(call.operands[0], sessionId, executor);
				break;
			case INSERT_INTO:
				operation = new InsertOperation(call.operands[0], sessionId, executor);
				break;
			case SHOW_MODULES:
				operation = new ShowModuleOperation(sessionId, executor);
				break;
			case SHOW_CATALOGS:
				operation = new ShowCatalogOperation(sessionId, executor);
				break;
			case SHOW_CURRENT_CATALOG:
				operation = new ShowCurrentCatalogOperation(sessionId, executor);
				break;
			case SHOW_DATABASES:
				operation = new ShowDatabaseOperation(sessionId, executor);
				break;
			case SHOW_CURRENT_DATABASE:
				operation = new ShowCurrentDatabaseOperation(sessionId, executor);
				break;
			case SHOW_TABLES:
				operation = new ShowTableOperation(sessionId, executor);
				break;
			case SHOW_FUNCTIONS:
				operation = new ShowFunctionOperation(sessionId, executor);
				break;
			case DESCRIBE:
				operation = new DescribeOperation(call.operands[0], sessionId, executor);
				break;
			case EXPLAIN:
				operation = new ExplainOperation(call.operands[0], sessionId, executor);
				break;
			default:
				throw new RuntimeException("Unsupported command call " + call + ". This is a bug.");
		}

		return operation;
	}
}
