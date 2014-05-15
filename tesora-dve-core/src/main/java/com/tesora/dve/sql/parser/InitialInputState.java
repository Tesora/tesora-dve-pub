// OS_STATUS: public
package com.tesora.dve.sql.parser;

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
