package com.tesora.dve.variable;

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

import java.lang.reflect.Constructor;

import javax.xml.bind.annotation.adapters.XmlAdapter;


public class VariableHandlerAdapter<VariableHandlerType> extends XmlAdapter<String, VariableHandlerType> {

	@SuppressWarnings("unchecked")
	@Override
	public VariableHandlerType unmarshal(String variableHandlerClassName) throws Exception {
		Constructor<?> ctor;
		try {
			Class<?> variableHandlerClass = Class.forName(variableHandlerClassName);
			ctor = variableHandlerClass.getConstructor(new Class[] {});
		} catch (Exception e) {
			ctor = e.getClass().getConstructor(new Class[] {String.class, Exception.class});
			Exception wrappedException = (Exception) ctor.newInstance(
					new Object[] { "Cannot instantiate variable handler " + variableHandlerClassName, e});
			throw wrappedException;
		}
		return (VariableHandlerType) ctor.newInstance(new Object[] {});
	}

	@Override
	public String marshal(VariableHandlerType v) throws Exception {
		return v.getClass().getCanonicalName();
	}

}
