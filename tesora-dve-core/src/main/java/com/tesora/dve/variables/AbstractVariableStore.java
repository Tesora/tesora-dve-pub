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

import com.tesora.dve.exceptions.PECodingException;

public abstract class AbstractVariableStore implements VariableStore {

	public AbstractVariableStore() {
	}

	public abstract <Type> void setValue(VariableHandler<Type> vh, Type t);

	public abstract <Type> void addVariable(VariableHandler<Type> vh, Type t);
	
	@Override
	public <Type> Type getValue(VariableHandler<Type> vh) {
		ValueReference<Type> vr = (ValueReference<Type>) getReference(vh);
		if (vr == null)
			throw new PECodingException("Variable \'" + vh.getName() + "\' not found on " + System.identityHashCode(this));
		return vr.get();
	}
}
