package com.tesora.dve.variables;

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

import com.tesora.dve.exceptions.PEException;

public class ValueReference<Type> {
	
	private final VariableHandler<Type> variable;
	private Type value;
	
	public ValueReference(VariableHandler<Type> vh) {
		this(vh,null);
	}
	
	public ValueReference(VariableHandler<Type> vh, Type value) {
		this.variable = vh;
		this.value = value;
	}
	
	public ValueReference<Type> copy() {
		ValueReference<Type> out = new ValueReference<Type>(variable);
		out.set(get());
		return out;
	}
	
	public void set(Type t) {
		value = t;
	}
	
	public void set(String v) throws PEException {
		set(variable.toInternal(v));
	}
	
	protected void setInternal(Object t) {
		value = (Type) t;
	}
	
	public Type get() {
		return value;
	}
	
	public VariableHandler<Type> getVariable() {
		return variable;
	}
}