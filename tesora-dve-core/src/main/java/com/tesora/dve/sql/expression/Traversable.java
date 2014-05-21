package com.tesora.dve.sql.expression;

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

import com.tesora.dve.common.PEConstants;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.TransformException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.transform.CopyContext;

public abstract class Traversable {

	protected Traversable parent;
	
	public Traversable getParent() {
		return parent;
	}
	
	public void setParent(Traversable t) {
		parent = t;
	}

	protected void setParentOf(Traversable that) {
		if (that != null)
			that.setParent(this);
	}
	
	protected void setParentOf(Collection<? extends Traversable> those) {
		if (those == null)
			return;
		for(Traversable t : those)
			setParentOf(t);
	}
	
	public void replace(Traversable prev, Traversable nv) {
		if (!replaceTraversable(prev, nv))
			throw new TransformException(Pass.REWRITER, "Unable to replace " + prev + " with " + nv + " in " + this);
	}

	public boolean replaceTraversable(Traversable prev, Traversable nv) {
		throw new IllegalStateException("Cannot replace in " + this.getClass().getName());
	}
		
	public Traversable copy(CopyContext cc) {
		throw new IllegalArgumentException("No copy support for " + this.getClass().getName());
	}
	
	public boolean hasAncestorsAmong(Collection<Traversable> ancestors) {
		if (parent == null)
			return false;
		if (ancestors.contains(parent))
			return true;
		else
			return parent.hasAncestorsAmong(ancestors);
	}

	@SuppressWarnings("unchecked")
	public <T extends Traversable> T ifAncestor(Collection<T> any) {
		if (any.contains(this))
			return (T)this;
		else if (parent == null)
			return null;
		else
			return parent.ifAncestor(any);
	}
	
	
	public String dumpAncestorChain() {
		StringBuffer buf = new StringBuffer();
		Traversable t = this;
		int l = 0;
		while(t != null) {
			buf.append("a[").append(l).append("]: ").append(t).append(PEConstants.LINE_SEPARATOR);
			l++;
			t = t.getParent();
		}
		return buf.toString();
	}
	
	public Traversable getEnclosing(Class<?> searchFor, Class<?> stopAt) {
		Traversable p = this;
		while(p != null) {
			if (searchFor.isInstance(p))
				return p;
			else if (stopAt != null && stopAt.isInstance(p))
				return null;
			p = p.getParent();
		}
		return null;
	}
	

	public int getSchemaHashCode() {
		throw new SchemaException(Pass.REWRITER, "No schema hash available for " + getClass().getName());
	}
	
	public boolean schemaEqual(Traversable other) {
		throw new SchemaException(Pass.REWRITER, "No schema equality available for " + getClass().getName());
	}

	protected int addSchemaHash(int result, Traversable in) {
		return addSchemaHash(result, (in == null ? 0 : in.getSchemaHashCode()));
	}
	
	protected int addSchemaHash(int result, boolean v) {
		return addSchemaHash(result, (v ? 1231 : 1237));
	}
	
	protected int addSchemaHash(int result, int hc) {
		final int prime = 31;
		return prime * result + hc;
	}

}
