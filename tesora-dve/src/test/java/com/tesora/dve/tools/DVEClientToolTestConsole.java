package com.tesora.dve.tools;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import static org.junit.Assert.assertTrue;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.List;

import com.tesora.dve.common.PEBaseTest;
import com.tesora.dve.common.StringOutputStream;
import com.tesora.dve.exceptions.PECodingException;

public class DVEClientToolTestConsole extends PEBaseTest {

	private final CLIBuilder tool;
	private final OutputStream output = new StringOutputStream();

	private boolean closed = false;
	private long commandCounter = 0;

	public DVEClientToolTestConsole(final CLIBuilder tool) {
		this.tool = tool;
		this.tool.setPrintStream(new PrintStream(this.output));
	}

	public void executeCommand(final String command) {
		executeCommand(command, true);
	}

	public void executeCommand(final String command, final boolean validateOutput) {
		execute(command, null, validateOutput);
	}

	public void executeInteractiveCommand(final String command, final String answer) {
		executeInteractiveCommand(command, answer, true);
	}

	public void executeInteractiveCommand(final String command, final String answer, final boolean validateOutput) {
		executeCommand(command, validateOutput);
		execute(command, answer, validateOutput);
	}

	public void executeCommands(final List<String> commands, final boolean validateOutput) {
		for (final String line : commands) {
			executeCommand(line, validateOutput);
		}

		if (validateOutput) {
			assertValidConsoleOutput();
		}
	}

	public void assertValidConsoleOutput() {
		assertValidConsoleOutput("Check the console output for errors.");
	}

	public void assertValidConsoleOutput(final String message) {
		assertTrue(message + System.lineSeparator() + this.output.toString(), hasValidConsoleOutput());
	}

	public void dumpOutputToScreen() {
		dumpOutputToStream(System.out);
	}

	public void dumpOutputToStream(final PrintStream ps) {
		ps.println(this.output.toString());
	}

	public void close() {
		this.tool.close();
		this.closed = true;
	}

	public boolean isClosed() {
		return this.closed;
	}

	protected void execute(final String command, final String answer, final boolean validateOutput) {
		if (isClosed()) {
			throw new PECodingException("This console has already been closed.");
		}

		if ((this.commandCounter > 0) && validateOutput) {
			assertValidConsoleOutput("Could not execute the command. The tool is in an invalid state. Check the console output.");
		}
		if (answer != null) {
			this.tool.parseAnswer(command, answer);
		} else {
			this.tool.parseLine(command);
		}
		if (validateOutput) {
			assertValidConsoleOutput("Command executed, but finished with errors. Check the console output.");
		}
		++commandCounter;
	}

	protected boolean hasValidConsoleOutput() {
		final String consoleText = this.output.toString();
		return (!consoleText.isEmpty() && (consoleText.indexOf("Error") < 0));
	}

	protected OutputStream getOutputStream() {
		return this.output;
	}
}
