package com.tesora.dve.sql.schema;

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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.Traversable;
import com.tesora.dve.sql.schema.cache.Cacheable;
import com.tesora.dve.sql.schema.cache.PersistentCacheKey;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.schema.validate.ValidateResult;
import com.tesora.dve.sql.transform.CopyContext;
import com.tesora.dve.sql.util.Accessor;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListSet;

@SuppressWarnings({ "rawtypes", "unused" })
public abstract class Persistable<Transient, Persistent> extends Traversable implements Cacheable {

	protected Name name;
	protected Integer persistentID;
	protected final SchemaCacheKey<?> sck;
	
	public Persistable(SchemaCacheKey<?> sck) {
		this.sck = sck;
	}
		
	protected abstract Class<? extends CatalogEntity> getPersistentClass();
	protected abstract int getID(Persistent p);
	protected PersistentCacheKey getPersistentCacheKey(int id) {
		return new PersistentCacheKey(getPersistentClass(), id);
	}
	public PersistentCacheKey getPersistentCacheKey() {
		if (persistentID == null) return null;
		return new PersistentCacheKey(getPersistentClass(), persistentID.intValue());
	}
		
	@Override
	public SchemaCacheKey<?> getCacheKey() {
		return sck;
	}
	
	protected abstract Persistent lookup(SchemaContext sc) throws PEException;
	protected abstract Persistent createEmptyNew(SchemaContext sc) throws PEException;
	protected abstract void populateNew(SchemaContext sc, Persistent p) throws PEException;

	protected abstract Persistable<Transient, Persistent> load(SchemaContext sc, Persistent p) throws PEException;
	

	protected void updateExisting(SchemaContext sc, Persistent p) throws PEException {
	}
	
	protected boolean updating = false;
	protected void update(SchemaContext sc, Persistent p) throws PEException {
		if (persistentID == null) return;
		if (updating)
			return;
		try {
			updating = true;
			updateExisting(sc,p);
		} finally {
			updating = false;
		}
	}
	
	protected boolean isTemporary() {
		return false;
	}
	
	public Persistent persistTree(SchemaContext sc) throws PEException {
		return persistTree(sc,false);
	}
	
	@SuppressWarnings("unchecked")
	protected Persistent lookupForRefresh(SchemaContext sc, int id) throws PEException {
		PersistentCacheKey pck = getPersistentCacheKey(id);
		if (pck == null) return null;
		return (Persistent)sc.getCatalog().findByKey(pck);
	}
	
	@SuppressWarnings({ "unchecked" })
	public Persistent persistTree(SchemaContext sc, boolean forRefresh) throws PEException {
		Persistent already = getPersistent(sc,false);
		if (already == null) {
			if (forRefresh) {
				if (persistentID != null) {
					already = lookupForRefresh(sc, persistentID);
					if (already != null) setPersistent(sc,already, getID(already));
				}
			} else {
				already = lookup(sc);
				if (already != null) {
					Persistable t = load(sc,already);
					if (t != null)
						verifySame(sc,t);
					setPersistent(sc,already,getID(already));
					update(sc,already);
				}
			}
			if (already == null) {
				if (!sc.isMutableSource()) { 
					throw new IllegalStateException("Creating an instance of type " + getPersistentClass().getName() + " on a nonmutable source");					
				}
				already = createEmptyNew(sc);
				setPersistent(sc,already, null);
				populateNew(sc,already);
			}
		} else {
			update(sc,already);
		}
		if (!isTemporary() && already instanceof CatalogEntity)
			sc.getSaveContext().add(this,(CatalogEntity)already);
		return already;
	}
	
	public Persistent getPersistent(SchemaContext pc) {
		return getPersistent(pc, true);
	}
	
	// get the persistent version, or null if none.
	@SuppressWarnings("unchecked")
	public Persistent getPersistent(SchemaContext pc, boolean create) {
		Persistent p = (Persistent)pc.getBacking(this);
		if (p instanceof CatalogEntity && persistentID != null) {
			CatalogEntity ce = (CatalogEntity) p;
			if (ce.getId() != persistentID.intValue())
				throw new SchemaException(Pass.PLANNER, "wrong persistent rep.  have " + persistentID + "@" + System.identityHashCode(this) + " but found " + ce.getId() + "@" + System.identityHashCode(ce));
		}
		if (!create)
			return p;
		if (p == null) {
			pc.beginSaveContext();
			try {
				persistTree(pc,true);
			} catch (PEException pe) {
				pe.printStackTrace();
				throw new SchemaException(Pass.SECOND,"Persistent refresh failed",pe);
			} finally {
				pc.endSaveContext();
			}
			return (Persistent)pc.getBacking(this);
		}
		return p;
	}

	protected void setPersistent(SchemaContext sc, Persistent p, Integer pid) {
		if (p == null) return;
		persistentID = pid;
		if (sc != null)	sc.setBacking(this, (Serializable)p);
	}
	
	
	public Persistable<Transient, Persistent> reload(SchemaContext toContext) throws PEException {
		throw new PEException("Cannot reload " + this.getClass().getSimpleName());
	}
	
	public Name getName() {
		return name;
	}

	public void setName(Name n) {
		name = n;
	}

	@SuppressWarnings("unchecked")
	public Transient get() {
		return (Transient)this;
	}
	
	public static final Accessor<Name, Persistable> getName = new Accessor<Name, Persistable>() {

		@Override
		public Name evaluate(Persistable object) {
			return object.getName();
		}
		
	};
	
	// the differs api, for checking equivalence between two schema objects which may be created different ways
	public String differs(SchemaContext sc, Persistable<Transient, Persistent> other, boolean first) {
		ArrayList<String> diffs = new ArrayList<String>();
		Set<Persistable> visited = new HashSet<Persistable>();
		collectDifferences(sc, diffs, other, first, visited);
		if (diffs.isEmpty())
			return null;
		StringBuilder buf = new StringBuilder();
		Functional.join(diffs, buf, "; ");
		return buf.toString();
	}

	// return true if any differences were found.
	public boolean collectDifferences(SchemaContext sc, List<String> messages, Persistable<Transient, Persistent> other, boolean first, Set<Persistable> visited) {
		return false;
	}
	
	protected abstract String getDiffTag();
	
	@SuppressWarnings({ "unchecked" })
	protected boolean maybeBuildDiffMessage(SchemaContext sc, List<String> messages, String what, Object current, Object other, boolean first, Set<Persistable> visited) {
		if (current == null && other == null)
			return false;
		if (current != null && other == null) {
			messages.add(getDiffTag() + " is missing " + what + ".  Did not find " + current);
			if (first)
				return true;
		}
		if (current == null && other != null) {
			messages.add(getDiffTag() + " has extra " + what + ".  Found extra " + other);
			if (first)
				return true;
		}
		if (current instanceof Persistable && other instanceof Persistable) 
			return ((Persistable)current).collectDifferences(sc, messages, (Persistable) other, first, visited);
		if (!current.equals(other)) {
			messages.add(getDiffTag() + " has different " + what + ".  Expected: " + current + ", but found: " + other);
			if (first)
				return true;
		}
		return false;
	}

	@SuppressWarnings({ "unchecked" })
	protected boolean maybeBuildDiffMessage(SchemaContext sc, List<String> messages, String what, 
			Map<Name, ? extends Persistable<?,?>> current, 
			Map<Name, ? extends Persistable<?,?>> other, 
			boolean first, Set<Persistable> visited) {
		if (current.size() != other.size()) {
			messages.add(getDiffTag() + " has different numbers of " + what + "s.  Expected: " + current.size() + ", but found: " + other.size());
			if (first)
				return true;
		}
		Map matching = new HashMap();
		for(Name n : current.keySet()) 
			matching.put(current.get(n), other.get(n));
		// purposefully misses items new in other - skipping for now
		for(Iterator iter = matching.entrySet().iterator(); iter.hasNext(); ) {
			Map.Entry me = (Map.Entry)iter.next();
			Persistable l = (Persistable)me.getKey();
			Persistable r = (Persistable)me.getValue();
			if (r == null) {
				messages.add(what + " with name " + l.getName().getSQL() + " is missing.");
				if (first)
					return true;
			} else if (l.collectDifferences(sc, messages, r, first, visited)) {
				return true;
			}
		}
		return false;
	}
	
	public void verifySame(SchemaContext sc, Persistable<Transient, Persistent> other) throws PEException {
		String diffs = differs(sc, other, true);
		if (diffs != null)
			throw new PEException(diffs);
	}		
	
	public Integer getPersistentID() {
		return persistentID;
	}
	
	// make sure we never make a copy without knowing the schema context
	@Override
	public final Traversable copy(CopyContext cc) {
		throw new IllegalArgumentException("No copy support for " + this.getClass().getName());
	}

	public Traversable copy(SchemaContext sc, CopyContext cc) {
		throw new IllegalArgumentException("No copy support for " + this.getClass().getName());
	}
	
	// the validation api - checks self consistency
	public void checkValid(SchemaContext sc, List<ValidateResult> acc) {
	}
	
	public List<ValidateResult> validate(SchemaContext sc, boolean complain) {
		ArrayList<ValidateResult> out = new ArrayList<ValidateResult>();
		checkValid(sc,out);
		if (complain) {
			String message = ValidateResult.buildMessage(sc, out, true);
			throw new SchemaException(Pass.NORMALIZE, message);
		}
		return out;
	}

	@Override
	public boolean equals(Object o) {
		if (sck == null) return super.equals(o);
		if (o instanceof Persistable) {
			Persistable<?,?> op = (Persistable<?, ?>) o;
			if (op.sck == null) return super.equals(o);
			return sck.equals(op.sck);
		}
		return super.equals(o);
	}

	@Override
	public int hashCode() {
		if (sck == null) return super.hashCode();
		return sck.hashCode();
	}
	
}
