package com.tesora.dve.common.logutil;

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


public class DisabledExecutionLogger implements ExecutionLogger {

	public static final DisabledExecutionLogger defaultDisabled = new DisabledExecutionLogger();
	
	@Override
	public void begin() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void end() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean completed() {
		return false;
	}

	@Override
	public long getDelta() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isEnabled() {
		return false;
	}

	@Override
	public LogSubject getSubject() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ExecutionLogger getNewLogger(LogSubject subject) {
		return defaultDisabled;
	}

	@Override
	public ExecutionLogger getNewLogger(String s) {
		return defaultDisabled;
	}
}
