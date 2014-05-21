// OS_STATUS: public
package com.tesora.dve.sql.node;

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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.tesora.dve.sql.schema.SchemaContext;

// per node container of derived attributes
public class EngineBlock {

	public enum DerivedAttributeState {
		NOT_APPLICABLE,
		HAS_VALUE
	};
	
	private final LanguageNode parentNode;
	private final Map<DerivedAttribute<?>, AttributeState<?>> derivedAttributes;
	
	private final Map<Object, Object> storage;
	
	public EngineBlock(LanguageNode forNode) {
		parentNode = forNode;
		derivedAttributes = new HashMap<DerivedAttribute<?>,AttributeState<?>>();
		storage = new HashMap<Object, Object>();
	}

	private <T> AttributeState<T> getStateForAttribute(DerivedAttribute<T> attr, SchemaContext sc) {
		@SuppressWarnings("unchecked")
		AttributeState<T> s = (AttributeState<T>) derivedAttributes.get(attr);
		if (s == null) {
			// missing state indicates we have never run it
			if (attr.isApplicableSubject(parentNode)) {
				s = new AttributeState<T>(attr.computeValue(sc, parentNode));
			} else {
				s = new AttributeState<T>();
			}
			derivedAttributes.put(attr,s);
		}
		return s;
	}
	
	public <T> T getValue(DerivedAttribute<T> attr, SchemaContext sc) {
		AttributeState<T> s = getStateForAttribute(attr, sc);
		if (!s.isApplicable()) return null;
		return s.getValue();
	}
	
	@SuppressWarnings("rawtypes")
	public <T> boolean hasValue(DerivedAttribute<T> attr, SchemaContext sc) {
		AttributeState<T> s = getStateForAttribute(attr,sc);
		if (!s.isApplicable()) return false;
		Object v = s.getValue();
		if (v == null) return false;
		if (v instanceof Collection && ((Collection)v).isEmpty()) return false;
		return true;
	}
	
	public void store(Object key, Object value) {
		storage.put(key, value);
	}
	
	public Object getFromStorage(Object key) {
		return storage.get(key);
	}
	
	public Object getAndClearFromStorage(Object key) {
		Object out = getFromStorage(key);
		clearFromStorage(key);
		return out;
	}
	
	public void clearFromStorage(Object key) {
		storage.remove(key);
	}
	
	public void clear() {
		storage.clear();
		derivedAttributes.clear();
	}
	
	private static class AttributeState<T> {

		private boolean applicable;
		private T value;
		
		public AttributeState() {
			applicable = false;
			value = null;
		}
		
		public AttributeState(T v) {
			applicable = true;
			value = v;
		}
		
		public boolean isApplicable() {
			return applicable;
		}
		
		public T getValue() {
			return value;
		}
	}
}
