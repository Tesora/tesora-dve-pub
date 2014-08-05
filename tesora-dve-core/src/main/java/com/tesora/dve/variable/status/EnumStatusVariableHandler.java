package com.tesora.dve.variable.status;

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

public abstract class EnumStatusVariableHandler<T> extends StatusVariableHandler {

	protected final T counter;
	
	public EnumStatusVariableHandler(String name, T counter) {
		super(name);
		this.counter = counter;
	}
	
	@Override
	protected String getValueInternal() throws Throwable {
		return Long.toString(getCounterValue(counter));
	}

	@Override
	protected void resetValueInternal() throws Throwable {
		resetCounterValue(counter);
	}
	
	protected T getCounter() {
		return counter;
	}
	
	protected abstract long getCounterValue(T counter) throws Throwable;
	
	protected abstract void resetCounterValue(T counter) throws Throwable;
	
}
