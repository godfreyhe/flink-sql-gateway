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

package com.ververica.flink.table.client.cli;

import com.ververica.flink.table.client.SqlClientException;
import com.ververica.flink.table.client.cli.SqlCommandParser.SqlCommandCall;
import com.ververica.flink.table.config.Environment;
import com.ververica.flink.table.gateway.SqlExecutionException;
import com.ververica.flink.table.rest.SessionClient;
import com.ververica.flink.table.rest.SqlRestException;
import com.ververica.flink.table.rest.message.StatementExecuteResponseBody;
import com.ververica.flink.table.rest.result.ResultSet;
import com.ververica.flink.table.rest.result.ResultSetUtil;
import com.ververica.flink.table.rest.result.TableSchemaUtil;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.MaskingCallback;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.jline.utils.InfoCmp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOError;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * SQL CLI client.
 */
public class CliClient {

	private static final Logger LOG = LoggerFactory.getLogger(CliClient.class);

	private final SessionClient sessionClient;

	private Environment environment;

	private final Terminal terminal;

	private final LineReader lineReader;

	private final String prompt;

	private boolean isRunning;

	private static final int PLAIN_TERMINAL_WIDTH = 80;

	private static final int PLAIN_TERMINAL_HEIGHT = 30;

	private static final int SOURCE_MAX_SIZE = 50_000;

	// the minimal sessionClient timeout is 30min,
	// the heartbeat interval is 15min
	private static final int HEARTBEAT_INTERVAL = 900_000;

	private ScheduledExecutorService scheduledExecutor;
	private ScheduledFuture heartbeatFuture;

	private volatile boolean isConnected = true;

	/**
	 * Creates a CLI instance with a custom terminal. Make sure to close the CLI instance
	 * afterwards using {@link #close()}.
	 */
	@VisibleForTesting
	public CliClient(Terminal terminal, SessionClient sessionClient, Environment environment) {
		this.terminal = terminal;
		this.sessionClient = sessionClient;
		this.environment = environment;

		// make space from previous output and test the writer
		terminal.writer().println();
		terminal.writer().flush();

		// initialize line lineReader
		lineReader = LineReaderBuilder.builder()
			.terminal(terminal)
			.appName(CliStrings.CLI_NAME)
			.parser(new SqlMultiLineParser())
			.completer(new SqlCompleter(sessionClient))
			.build();
		// this option is disabled for now for correct backslash escaping
		// a "SELECT '\'" query should return a string with a backslash
		lineReader.option(LineReader.Option.DISABLE_EVENT_EXPANSION, true);
		// set strict "typo" distance between words when doing code completion
		lineReader.setVariable(LineReader.ERRORS, 1);
		// perform code completion case insensitive
		lineReader.option(LineReader.Option.CASE_INSENSITIVE, true);

		// create prompt
		prompt = new AttributedStringBuilder()
			.style(AttributedStyle.DEFAULT.foreground(AttributedStyle.GREEN))
			.append("Flink SQL")
			.style(AttributedStyle.DEFAULT)
			.append("> ")
			.toAnsi();
	}

	/**
	 * Creates a CLI instance with a prepared terminal. Make sure to close the CLI instance
	 * afterwards using {@link #close()}.
	 */
	public CliClient(SessionClient sessionClient, Environment environment) {
		this(createDefaultTerminal(), sessionClient, environment);
	}

	public Terminal getTerminal() {
		return terminal;
	}

	public SessionClient getSessionClient() {
		return this.sessionClient;
	}

	public Environment getEnvironment() {
		return environment;
	}

	public void clearTerminal() {
		if (isPlainTerminal()) {
			for (int i = 0; i < 200; i++) { // large number of empty lines
				terminal.writer().println();
			}
		} else {
			terminal.puts(InfoCmp.Capability.clear_screen);
		}
	}

	public boolean isPlainTerminal() {
		// check if terminal width can be determined
		// e.g. IntelliJ IDEA terminal supports only a plain terminal
		return terminal.getWidth() == 0 && terminal.getHeight() == 0;
	}

	public int getWidth() {
		if (isPlainTerminal()) {
			return PLAIN_TERMINAL_WIDTH;
		}
		return terminal.getWidth();
	}

	public int getHeight() {
		if (isPlainTerminal()) {
			return PLAIN_TERMINAL_HEIGHT;
		}
		return terminal.getHeight();
	}

	/**
	 * Opens the interactive CLI shell.
	 */
	public void open() {
		isRunning = true;

		// TODO init gateway session properties based on local environment.

		// print welcome
		terminal.writer().append(CliStrings.MESSAGE_WELCOME);

		startSendHeartbeat();

		// begin reading loop
		while (isRunning) {
			// make some space to previous command
			terminal.writer().append("\n");
			terminal.flush();

			final String line;
			try {
				line = lineReader.readLine(prompt, null, (MaskingCallback) null, null);
			} catch (UserInterruptException e) {
				// user cancelled line with Ctrl+C
				continue;
			} catch (EndOfFileException | IOError e) {
				// user cancelled application with Ctrl+D or kill
				break;
			} catch (Throwable t) {
				throw new SqlClientException("Could not read from command line.", t);
			}
			if (line == null) {
				continue;
			}
			final Optional<SqlCommandCall> cmdCall = parseCommand(line);
			cmdCall.ifPresent((cmd) -> callCommand(cmd, line));
		}
	}

	/**
	 * Closes the CLI instance.
	 */
	public void close() {
		if (heartbeatFuture != null) {
			heartbeatFuture.cancel(true);
		}
		if (scheduledExecutor != null) {
			scheduledExecutor.shutdown();
		}
		try {
			terminal.close();
		} catch (IOException e) {
			throw new SqlClientException("Unable to close terminal.", e);
		}
	}

	/**
	 * Submits a SQL update statement and prints status information and/or errors on the terminal.
	 *
	 * @param statement SQL update statement
	 * @return flag to indicate if the submission was successful or not
	 */
	public boolean submitUpdate(String statement) throws SqlExecutionException {
		terminal.writer().println(CliStrings.messageInfo(CliStrings.MESSAGE_WILL_EXECUTE).toAnsi());
		terminal.writer().println(new AttributedString(statement).toString());
		terminal.flush();

		StatementExecuteResponseBody responseBody;
		try {
			responseBody = sessionClient.submitStatement(statement);
		} catch (SqlRestException e) {
			throw new SqlExecutionException("Failed to submit statement");
		}

		Preconditions.checkArgument(
			responseBody.getResults().size() == 1 &&
				responseBody.getStatementTypes().size() == 1,
			"We currently only support submitting one SQL statement at a time");

		SqlCommandParser.SqlCommand command =
			SqlCommandParser.SqlCommand.valueOf(responseBody.getStatementTypes().get(0));
		switch (command) {
			case INSERT_INTO:
				printInfo(CliStrings.MESSAGE_SUBMITTING_STATEMENT);
				try {
					JobID jobId = ResultSetUtil.getJobID(responseBody.getResults().get(0));
					terminal.writer().println(CliStrings.messageInfo(CliStrings.MESSAGE_STATEMENT_SUBMITTED).toAnsi());
					terminal.writer().println(jobId);
					terminal.flush();
				} catch (SqlExecutionException e) {
					printExecutionException(e);
					return false;
				}
				return true;
			default:
				printError(CliStrings.MESSAGE_UNSUPPORTED_SQL);
				return false;
		}
	}

	// --------------------------------------------------------------------------------------------

	private Optional<SqlCommandCall> parseCommand(String line) {
		final Optional<SqlCommandCall> parsedLine = SqlCommandParser.parse(line);
		if (!parsedLine.isPresent()) {
			printError(CliStrings.MESSAGE_UNKNOWN_SQL);
		}
		return parsedLine;
	}

	private void callCommand(SqlCommandCall cmdCall, String originStatement) {
		switch (cmdCall.command) {
			case QUIT:
				callQuit();
				break;
			case CLEAR:
				callClear();
				break;
			case HELP:
				callHelp();
				break;
			case SOURCE:
				callSource(cmdCall);
				break;
			case CONNECT:
				callConnect();
				break;
			default:
				callOther(originStatement);
		}
	}

	private void callOther(String statement) {
		boolean needCancelJob = false;
		ResultSet resultSet = null;
		try {
			StatementExecuteResponseBody responseBody = sessionClient.submitStatement(statement);
			Preconditions.checkArgument(
				responseBody.getResults().size() == 1 &&
					responseBody.getStatementTypes().size() == 1,
				"We currently only support submitting one SQL statement at a time");

			resultSet = responseBody.getResults().get(0);
			if (resultSet == null) {
				throw new SqlClientException("result is null");
			}
			SqlCommandParser.SqlCommand command =
				SqlCommandParser.SqlCommand.valueOf(responseBody.getStatementTypes().get(0));
			switch (command) {
				case RESET:
					printInfo(CliStrings.MESSAGE_RESET);
					break;
				case SET:
					// show all properties
					if (resultSet.getColumns().size() == 2) {
						if (resultSet.getData() == null || resultSet.getData().isEmpty()) {
							terminal.writer().println(CliStrings.messageInfo(CliStrings.MESSAGE_EMPTY).toAnsi());
						} else {
							resultSet
								.getData()
								.stream()
								.map((r) -> r.getField(0) + "=" + r.getField(1))
								.sorted()
								.forEach((p) -> terminal.writer().println(p));
						}
					} else {
						// TODO remove this hack solution
						Optional<SqlCommandCall> call = parseCommand(statement);
						String key = call.get().operands[0];
						String value = call.get().operands[1];
						environment = Environment.enrich(environment, ImmutableMap.of(key, value), ImmutableMap.of());
						printInfo(CliStrings.MESSAGE_SET);
					}
					break;
				case SHOW_CATALOGS:
				case SHOW_DATABASES:
				case SHOW_TABLES:
				case SHOW_FUNCTIONS:
				case SHOW_MODULES:
					List<String> values = SessionClient.singleStringColumn(resultSet.getData());
					if (values.isEmpty()) {
						terminal.writer().println(CliStrings.messageInfo(CliStrings.MESSAGE_EMPTY).toAnsi());
					} else {
						values.forEach((v) -> terminal.writer().println(v));
					}
					terminal.flush();
					break;
				case USE_CATALOG:
				case USE: // TODO USE_DATABASE
					break;
				case DESCRIBE:
					TableSchema schema = getTableSchema(resultSet.getData());
					terminal.writer().println(schema.toString());
					terminal.flush();
					break;
				case EXPLAIN:
					String explanation = SessionClient.singleStringColumn(resultSet.getData()).get(0);
					terminal.writer().println(explanation);
					terminal.flush();
					break;
				case CREATE_DATABASE:
					printInfo(CliStrings.MESSAGE_DATABASE_CREATED);
					break;
				case ALTER_DATABASE:
					printInfo(CliStrings.MESSAGE_DATABASE_ALTER_SUCCEEDED);
					break;
				case DROP_DATABASE:
					printInfo(CliStrings.MESSAGE_DATABASE_REMOVED);
					break;
				case CREATE_TABLE:
					printInfo(CliStrings.MESSAGE_TABLE_CREATED);
					break;
				case ALTER_TABLE:
					printInfo(CliStrings.MESSAGE_ALTER_TABLE_SUCCEEDED);
					break;
				case DROP_TABLE:
					printInfo(CliStrings.MESSAGE_TABLE_REMOVED);
					break;
				case CREATE_VIEW:
					printInfo(CliStrings.MESSAGE_VIEW_CREATED);
					break;
				case DROP_VIEW:
					printInfo(CliStrings.MESSAGE_VIEW_REMOVED);
					break;
				case INSERT_INTO:
					// TODO need this message ?
					printInfo(CliStrings.MESSAGE_SUBMITTING_STATEMENT);
					JobID insertJobId = ResultSetUtil.getJobID(resultSet);
					terminal.writer().println(CliStrings.messageInfo(CliStrings.MESSAGE_STATEMENT_SUBMITTED).toAnsi());
					terminal.writer().println(insertJobId.toString());
					terminal.flush();
					break;
				case SELECT:
					needCancelJob = true;
					CliResultView view;
					JobID selectJobId = ResultSetUtil.getJobID(resultSet);
					if (environment.getExecution().isChangelogMode()) {
						Preconditions.checkArgument(environment.getExecution().inStreamingMode());
						view = new CliChangelogResultView(this, selectJobId);
					} else {
						view = new CliTableResultView(this, selectJobId);
					}
					// enter view
					try {
						view.open();

						// view left
						printInfo(CliStrings.MESSAGE_RESULT_QUIT);
					} catch (SqlExecutionException e) {
						printExecutionException(e);
					}
					break;
				default:
					throw new SqlClientException("Unsupported command: " + command);
			}
		} catch (SqlExecutionException | SqlRestException | SqlClientException e) {
			printExecutionException(e);
			if (isConnectionRefused(e) || isSessionNotExist(e)) {
				terminal.writer().println(CliStrings.messageInfo(
					"Please connect to the gateway server via command: \'!connect\'.\n" +
						"[NOTE] a new sessionClient will be created, the previous sessionClient properties were cleared."
				).toAnsi());
				terminal.flush();
				isConnected = false;
			}
		} finally {
			if (needCancelJob) {
				try {
					JobID jobId = ResultSetUtil.getJobID(resultSet);
					sessionClient.cancelJob(jobId);
				} catch (Exception e) {
					// ignore the message
				}
			}
		}
	}

	private void callQuit() {
		printInfo(CliStrings.MESSAGE_QUIT);
		isRunning = false;
	}

	private void callClear() {
		clearTerminal();
	}

	private void callHelp() {
		terminal.writer().println(CliStrings.MESSAGE_HELP);
		terminal.flush();
	}

	private void callSource(SqlCommandCall cmdCall) {
		final String pathString = cmdCall.operands[0];

		// load file
		final String stmt;
		try {
			final Path path = Paths.get(pathString);
			byte[] encoded = Files.readAllBytes(path);
			stmt = new String(encoded, Charset.defaultCharset());
		} catch (IOException e) {
			printExecutionException(e);
			return;
		}

		// limit the output a bit
		if (stmt.length() > SOURCE_MAX_SIZE) {
			printExecutionError(CliStrings.MESSAGE_MAX_SIZE_EXCEEDED);
			return;
		}

		terminal.writer().println(CliStrings.messageInfo(CliStrings.MESSAGE_WILL_EXECUTE).toAnsi());
		terminal.writer().println(new AttributedString(stmt).toString());
		terminal.flush();

		// try to run it
		final Optional<SqlCommandCall> call = parseCommand(stmt);
		call.ifPresent((cmd) -> callCommand(cmd, stmt));
	}

	private void callConnect() {
		if (isConnected) {
			try {
				sessionClient.sendHeartbeat();
				printInfo("The connection is ok, ignore this command.");
				return;
			} catch (SqlRestException e) {
				if (isSessionNotExist(e)) {
					printException("Could not send heartbeat.", e);
					printInfo("Reconnecting to the gateway... \n"
						+ "[NOTE] A new sessionClient will be created, the previous sessionClient properties were cleared.");
				}
			}
		}
		try {
			sessionClient.reconnect();
			isConnected = true;
			printInfo("Reconnect to the gateway successfully.");
		} catch (Exception e) {
			isConnected = false;
			printException("Could not connect to the gateway.", e);
		}
	}

	private boolean isConnectionRefused(Exception e) {
		return e.getCause().getMessage().contains("Connection refused");
	}

	private boolean isSessionNotExist(Exception e) {
		String msg = "Session " + sessionClient.getSessionId() + " does not exist";
		return e.getCause().getMessage().contains(msg);
	}

	// --------------------------------------------------------------------------------------------

	private void startSendHeartbeat() {
		scheduledExecutor = Executors.newSingleThreadScheduledExecutor(
			new ExecutorThreadFactory("Heartbeat-Connection-IO"));
		heartbeatFuture = scheduledExecutor.scheduleAtFixedRate(() -> {
			try {
				sessionClient.sendHeartbeat();
			} catch (SqlRestException e) {
				if (isConnectionRefused(e) || isSessionNotExist(e)) {
					isConnected = false;
				}
			}
		}, HEARTBEAT_INTERVAL, HEARTBEAT_INTERVAL, TimeUnit.MILLISECONDS);
	}

	// --------------------------------------------------------------------------------------------

	private void printExecutionException(Throwable t) {
		printExecutionException(null, t);
	}

	private void printExecutionException(String message, Throwable t) {
		final String finalMessage;
		if (message == null) {
			finalMessage = CliStrings.MESSAGE_SQL_EXECUTION_ERROR;
		} else {
			finalMessage = CliStrings.MESSAGE_SQL_EXECUTION_ERROR + ' ' + message;
		}
		printException(finalMessage, t);
	}

	private void printExecutionError(String message) {
		terminal.writer().println(CliStrings.messageError(CliStrings.MESSAGE_SQL_EXECUTION_ERROR, message).toAnsi());
		terminal.flush();
	}

	private void printException(String message, Throwable t) {
		LOG.warn(message, t);
		terminal.writer().println(CliStrings.messageError(message, t).toAnsi());
		terminal.flush();
	}

	private void printError(String message) {
		terminal.writer().println(CliStrings.messageError(message).toAnsi());
		terminal.flush();
	}

	private void printInfo(String message) {
		terminal.writer().println(CliStrings.messageInfo(message).toAnsi());
		terminal.flush();
	}

	// --------------------------------------------------------------------------------------------

	private TableSchema getTableSchema(List<Row> data) {
		String schemaJson = data.get(0).toString();
		try {
			return TableSchemaUtil.readTableSchemaFromJson(schemaJson);
		} catch (JsonProcessingException e) {
			throw new SqlClientException("Failed to deserialize json to TableSchema", e);
		}
	}

	// --------------------------------------------------------------------------------------------

	private static Terminal createDefaultTerminal() {
		try {
			return TerminalBuilder.builder()
				.name(CliStrings.CLI_NAME)
				.build();
		} catch (IOException e) {
			throw new SqlClientException("Error opening command line interface.", e);
		}
	}
}
