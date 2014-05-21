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

import java.util.Collections;
import java.util.List;

public class SingleEdge<O extends LanguageNode, T extends LanguageNode> extends Edge<O,T> {

	protected T target;
	protected boolean owning;
	
	public SingleEdge(Class<O> oc, O o, IEdgeName n) {
		this(oc, o, n, true);
	}
	
	public SingleEdge(Class<O> oc, O o, IEdgeName n, boolean owning) {
		super(oc, o, n);
		target = null;
		this.owning = owning;
	}
	
	@Override
	public boolean has() {
		return target != null;
	}

	@Override
	public boolean isMulti() {
		return false;
	}
	
	@Override
	public boolean isMultiMulti() {
		return false;
	}
	
	@Override
	public T get() {
		return target;
	}

	@Override
	public List<T> getMulti() {
		return Collections.singletonList(target);
	}

	@Override
	public void set(T in) {
		if (target != null) {
			target = null;
		}
		if (in == null) {
			target = null;
		} else {
			target = in;
			if (owning)
				target.setParent(this);
		}
	}

	@Override
	public void set(List<T> in) {
		if (in == null || in.isEmpty())
			set((T)null);
		else if (in.size() > 1) {
			throw new MigrationException("Too many parameters to single edge: collection of size " + in.size());
		} else {
			set(in.get(0));
		}
	}

	
	@Override
	public void clear() {
		set((T)null);
	}
	
	@Override
	public boolean schemaEqual(Edge<O,T> other) {
		if (other == null) return false;
		if (!(other instanceof SingleEdge)) return false;
		LanguageNode mln = get();
		LanguageNode oln = other.get();
		if (mln == null && oln == null) return true;
		else if (mln != null && oln != null && mln.isSchemaEqual(oln)) return true;
		return false;
	}
	
	@Override
	public void add(T in) {
		set(in);
	}

	@Override
	public void add(List<T> in) {
		set(in);
	}

	@Override
	protected int schemaTargetHash() {
		if (get() == null) return 0;
		return get().getSchemaHashCode();
	}

}
