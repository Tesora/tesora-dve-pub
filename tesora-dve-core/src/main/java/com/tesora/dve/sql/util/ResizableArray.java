// OS_STATUS: public
package com.tesora.dve.sql.util;

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

import java.util.Arrays;

// different than an array list - in particular inserts into the middle don't shift items
public class ResizableArray<T> {

	private Object[] backing;
	private int last;
	
	public ResizableArray() {
		this(10);
	}
	
	public ResizableArray(int initCapacity) {
		backing = new Object[initCapacity];
		last = 0;
	}
	
	public int size() {
		return last;
	}
	
	@SuppressWarnings("unchecked")
	public T get(int i) {
		if (i >= backing.length)
			return null;
		return (T) backing[i];
	}
	
	public void set(int i, T val) {
		if (i >= backing.length) 
			reallocate(i - backing.length + 1);
		backing[i] = val;
		int nl = i+1;
		if (nl > last)
			last = nl;
	}
	
	private void reallocate(int delta) {
		int oldLength = backing.length;
		int newLength = (oldLength * 3)/2 + 1;
		if (newLength < (oldLength + delta))
			newLength = oldLength + delta;
		backing = Arrays.copyOf(backing, newLength);
	}
	
}
