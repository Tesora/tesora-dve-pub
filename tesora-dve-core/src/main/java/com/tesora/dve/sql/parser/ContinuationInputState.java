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

public class ContinuationInputState implements InputState {

	// the backing char array
	private final char[] buffer;
	// and the number of actual chars, as measured by string
	private final int numberOfActualCharacters;
	// at the time of construction, the value of the max line length
	private final long cutoff;
	// at the time of construction, was this an insert?
	private final boolean insert;
	// and our current position in it
	private int position;
	// and the skeleton
	private InsertIntoValuesStatement skeleton = null;

	public ContinuationInputState(String firstTime, long threshold) {
		buffer = firstTime.toCharArray();
		numberOfActualCharacters = firstTime.length();
		position = 0;
		cutoff = threshold;
		insert = CandidateParser.isInsert(firstTime); 
	}
	
	@Override
	public ANTLRStringStream buildNewStream() {
		ANTLRStringStream line = new ANTLRStringStream(buffer,numberOfActualCharacters);
		if (position > 0)
			line.seek(position);
		return line;
	}
	
	@Override
	public void setCurrentPosition(int pos) {
		position = pos;
	}

	@Override
	public int getCurrentPosition() {
		return position;
	}
	
	@Override
	public String describe() {
		return "ContinuationInputLine(" + position + " out of " + numberOfActualCharacters + " chars)";
	}

	@Override
	public String getCommand() {
		return null;
	}

	@Override
	public void setInsertSkeleton(InsertIntoValuesStatement is) {
		skeleton = is;
	}

	@Override
	public InsertIntoValuesStatement getInsertSkeleton() {
		return skeleton;
	}
	
	@Override
	public long getThreshold() {
		return cutoff;
	}

	@Override
	public boolean isInsert() {
		return insert;
	}
	
}
