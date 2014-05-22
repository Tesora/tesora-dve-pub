package com.tesora.dve.common;

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
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.tesora.dve.exceptions.PEException;

public class PEProperties extends Properties implements Cloneable {

	private static final long serialVersionUID = 1L;

	private Map<Object, Object> linkMap = new LinkedHashMap<Object, Object>();

	@Override
	public Object put(Object key, Object value) {
		return linkMap.put(key, value);
	}

	@Override
	public void putAll(Map<? extends Object, ? extends Object> map) {
		linkMap.putAll(map);
	}

	@Override
	public Object get(Object key) {
		return linkMap.get(key);
	}

	@Override
	public String getProperty(String key) {
		Object oval = linkMap.get(key);
		String sval = (oval instanceof String) ? (String) oval : null;
		return ((sval == null) && (defaults != null)) ? defaults.getProperty(key) : sval;
	}
	
	public int getIntegerProperty(String key, int defVal) throws PEException {
		try {
			return Integer.parseInt(getProperty(key, Integer.toString(defVal)));
		} catch (NumberFormatException nfe) {
			throw new PEException("Failed to parse property value for " + key);
		}
	}
	
	@Override
	public Object remove(Object key) {
		return linkMap.remove(key);
	}

	@Override
	public int size() {
		return linkMap.size();
	}

	@Override
	public boolean isEmpty() {
		return size() == 0;
	}

	@Override
	public boolean contains(Object value) {
		return linkMap.containsKey(value);
	}

	@Override
	public boolean containsValue(Object value) {
		return linkMap.containsValue(value);
	}

	@Override
	public boolean containsKey(Object key) {
		return linkMap.containsKey(key);
	}

	@Override
	public Enumeration<Object> elements() {
		return Collections.enumeration(linkMap.values());
	}

	@Override
	public Enumeration<Object> keys() {
		return Collections.enumeration(linkMap.keySet());
	}

	@Override
	public Set<Map.Entry<Object, Object>> entrySet() {
		return linkMap.entrySet();
	}

	@Override
	public Set<Object> keySet() {
		return linkMap.keySet();
	}

	@Override
	public Collection<Object> values() {
		return linkMap.values();
	}

	@Override
	public boolean equals(Object o) {
		return linkMap.equals(o);
	}

	@Override
	public int hashCode() {
		return linkMap.hashCode();
	}

	@Override
	public void clear() {
		linkMap.clear();
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public Object clone() {
		PEProperties ret = (PEProperties)super.clone();
		
		ret.linkMap = (LinkedHashMap<Object, Object>)((LinkedHashMap<Object, Object>)linkMap).clone();
		
		return ret;
		
	}
}