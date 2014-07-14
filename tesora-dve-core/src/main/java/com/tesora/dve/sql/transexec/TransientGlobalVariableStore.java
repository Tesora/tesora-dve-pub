package com.tesora.dve.sql.transexec;

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

import com.tesora.dve.variables.GlobalVariableStore;
import com.tesora.dve.variables.LocalVariableStore;
import com.tesora.dve.variables.ValueReference;
import com.tesora.dve.variables.VariableHandler;
import com.tesora.dve.variables.AbstractVariableStore;
import com.tesora.dve.variables.VariableScope;

// non locking version, since the full stack is not available
public class TransientGlobalVariableStore extends AbstractVariableStore implements
		GlobalVariableStore {

	private ConcurrentHashMap<VariableHandler,ValueReference> values;
	
	public TransientGlobalVariableStore() {
		values = new ConcurrentHashMap<VariableHandler,ValueReference>();
	}
	
	@Override
	public void invalidate() {
		// does nothing
	}

	@Override
	public LocalVariableStore buildNewLocalStore() {
		LocalVariableStore out = new LocalVariableStore();
		for(ValueReference<?> vr : values.values()) {
			VariableHandler<?> vh = vr.getVariable();
			if (vh.getScopes().contains(VariableScope.SESSION)) {
				out.setInternal(vh,vr.get());
			}
		}
		return out;
	}

	@Override
	public boolean isServer() {
		return false;
	}

	@Override
	public <Type> ValueReference<Type> getReference(VariableHandler<Type> vh) {
		return values.get(vh);
	}

	@Override
	public <Type> void setValue(VariableHandler<Type> vh, Type t) {
		ValueReference<Type> vr = getReference(vh);
		if (vr == null) {
			vr = new ValueReference<Type>(vh,t);
			values.put(vh,vr);
		} else {
			vr.set(t);
		}
	}

}
