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

import java.util.concurrent.ConcurrentHashMap;

public class LocalVariableStore extends AbstractVariableStore {

	private final ConcurrentHashMap<String, ValueReference<?>> values;
	
	public LocalVariableStore() {
		super();
		values = new ConcurrentHashMap<String, ValueReference<?>>();
	}

	@SuppressWarnings("unchecked")
	@Override
	public <Type> ValueReference<Type> getReference(VariableHandler<Type> vh) {
		return (ValueReference<Type>) values.get(vh.getName());
	}

	@SuppressWarnings("unchecked")
	@Override
	public <Type> void setValue(VariableHandler<Type> vh, Type t) {
		// no locking
		ValueReference<Type> vr = (ValueReference<Type>) values.get(vh.getName());
		if (vr == null) {
			vr = new ValueReference<Type>(vh);
			values.put(vh.getName(),vr);
		}
		vr.set(t);
	}

	public void setInternal(VariableHandler<?> vh, Object o) {
		ValueReference<?> vr = new ValueReference(vh,o);
		values.put(vh.getName(),vr);
	}
	
}
