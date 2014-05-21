// OS_STATUS: public
package com.tesora.dve.sql.parser;

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

import org.antlr.runtime.ANTLRStringStream;

import com.tesora.dve.sql.statement.dml.InsertIntoValuesStatement;

public class InitialInputState implements InputState {

	private final String cmd;
	
	public InitialInputState(String cmd) {
		this.cmd = cmd;
	}

	@Override
	public ANTLRStringStream buildNewStream() {
		return new ANTLRStringStream(cmd);
	}

	@Override
	public String describe() {
		return cmd;
	}

	@Override
	public void setCurrentPosition(int pos) {
		if (pos > -1)
			throw new IllegalArgumentException("Invalid input state for more input");
	}

	@Override
	public int getCurrentPosition() {
		return 0;
	}
	
	@Override
	public String getCommand() {
		return cmd;
	}

	@Override
	public void setInsertSkeleton(InsertIntoValuesStatement is) {
		throw new IllegalArgumentException("Non continuation input state cannot accept a skeleton");
	}

	@Override
	public InsertIntoValuesStatement getInsertSkeleton() {
		return null;
	}
	
	@Override
	public long getThreshold() {
		return Long.MAX_VALUE;
	}
	
	@Override
	public boolean isInsert() {
		return CandidateParser.isInsert(cmd.trim());
	}
	
}
