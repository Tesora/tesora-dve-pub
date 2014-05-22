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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.schema.PEAbstractTable.TableCacheKey;
import com.tesora.dve.sql.schema.cache.CacheAwareLookup;
import com.tesora.dve.sql.schema.cache.NamedEdge;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.schema.cache.SchemaEdge;

public class PESchema extends Persistable<PESchema, Map<String,UserTable>> implements Schema<PEAbstractTable<?>> {

	protected CacheAwareLookup<PEAbstractTable<?>> lookup;
	protected SchemaEdge<PEDatabase> pdb;
	private boolean dirty;
	
	private boolean loaded = false;
	
	@SuppressWarnings("unchecked")
	public PESchema(SchemaContext pc, PEDatabase pdb) {
		super(null);
		this.pdb = StructuralUtils.buildEdge(pc,pdb,false);
		setPersistent(pc,null, null);
        lookup = Singletons.require(HostService.class).getDBNative().getEmitter().getTableLookup();
		dirty = true;
		loaded = true;
	}
	
	protected PESchema(SchemaContext pc) {
		this(pc,null);
	}
	
	@SuppressWarnings("unchecked")
	public PESchema(UserDatabase db, SchemaContext lc, PEDatabase pedb) {
		super(null);
		this.pdb = StructuralUtils.buildEdge(lc,pedb,true);
        this.lookup = Singletons.require(HostService.class).getDBNative().getEmitter().getTableLookup();
		dirty = false;
		loaded = false;
	}
	
	private void checkLoaded(SchemaContext pc) {
		if (loaded)
			return;
		loaded = true;
		Map<String,UserTable> tabs = new HashMap<String, UserTable>();
		setPersistent(pc, tabs, null);
		PEDatabase adb = pdb.get(pc);
		if (adb != null && adb.getPersistent(pc) != null) {
			for(UserTable ut : adb.getPersistent(pc).getUserTables()) {
				tabs.put(ut.getName(), ut);
			}
		}
		for(UserTable ut : tabs.values()) {
			PEAbstractTable<?> pet = PEAbstractTable.load(ut, pc);
			lookup.add(pc, pet,true);
			pet.setDatabase(pc,adb,true);
		}
	}
	
	@Override
	public PEAbstractTable<?> addTable(SchemaContext pc, PEAbstractTable<?> t) {
		checkLoaded(pc);
		PEAbstractTable<?> already = lookup.lookup(pc, t.getName());
		if (already != null) return already;
		lookup.add(pc,t,false);
		PEDatabase adb = pdb.get(pc);
		if (adb != null)
			t.setDatabase(pc,adb,false);
		dirty = true;
		t.setDatabase(pc,adb,false);
		return t;
	}
	
	@Override
	public Collection<PEAbstractTable<?>> getTables(SchemaContext pc) {
		throw new IllegalStateException("PESchema.getTables?");
	}
	
	@Override
	public TableInstance buildInstance(SchemaContext pc, UnqualifiedName n, LockInfo lockInfo, boolean domtchecks) {
		TableInstance t = null;
		if (domtchecks && pc.getPolicyContext().isSchemaTenant()) {
			t = pc.getPolicyContext().buildInCurrentTenant(this,n, lockInfo);
		} else {
			PEAbstractTable<?> pet = lazyLookup(pc,n, lockInfo);
			if (pet != null)
				t = new TableInstance(pet,pc.getOptions().isResolve());
		}
		return t;
		
	}
	
	private PEAbstractTable<?> lazyLookup(SchemaContext pc, UnqualifiedName n, LockInfo lockType) {
		PEAbstractTable<?> targ = lookup.lookup(pc,n);
		if (targ == null) {
			PEDatabase adb = pdb.get(pc);
			if (adb != null) {
				TableCacheKey tck = PEAbstractTable.getTableKey(adb, n);
				tck.acquireLock(pc, lockType);
				targ = pc.findTable(tck);
				if (targ == null) return null;
				lookup.add(pc, targ,true);
			}
		} else {
			targ.getCacheKey().acquireLock(pc, lockType);
		}
		return targ;
	}
	
	@Override
	public TableInstance buildInstance(SchemaContext pc, UnqualifiedName n, LockInfo lockType) {
		return buildInstance(pc, n, lockType, true);
	}
	
	protected boolean isDirty() {
		return dirty;
	}
	
	protected void setDirty(SchemaContext pc, boolean v) {
		checkLoaded(pc);
		dirty = v;
	}
	
	@Override
	public UnqualifiedName getName() {
		throw new IllegalStateException("Schemas do not have names");
	}

	@Override
	public UnqualifiedName getSchemaName(SchemaContext sc) {
		return pdb.get(sc).getName().getUnqualified();
	}
	
	@Override
	public boolean collectDifferences(SchemaContext sc, List<String> messages, Persistable<PESchema, Map<String,UserTable>> other,
			boolean first, @SuppressWarnings("rawtypes") Set<Persistable> visited) {
		throw new IllegalStateException("Schemas do not have differences.");
	}

	@Override
	protected String getDiffTag() {
		return "Schema";
	}

	@Override
	public Persistable<PESchema, Map<String, UserTable>> reload(
			SchemaContext usingContext) {
		throw new IllegalStateException("Cannot reload a schema");
	}
	
	@Override
	protected int getID(Map<String, UserTable> p) {
		throw new IllegalStateException("Schema has no persistent id");
	}

	@Override
	protected Map<String, UserTable> lookup(SchemaContext pc) throws PEException {
		return null;
	}

	@Override
	protected Map<String, UserTable> createEmptyNew(SchemaContext pc) throws PEException {
		Map <String,UserTable> tabs = new HashMap<String, UserTable>();
		return tabs;
	}

	@Override
	protected void populateNew(SchemaContext pc, Map<String, UserTable> p) throws PEException {
		for(NamedEdge<PEAbstractTable<?>> nt : lookup.getValues()) {
			// since we only create on mutable sources, this should always return the table
			PEAbstractTable<?> t = nt.getEdge().get(pc);
			UserTable ut = t.persistTree(pc);
			p.put(ut.getName(), ut);
		}
	}

	@Override
	protected Persistable<PESchema, Map<String, UserTable>> load(SchemaContext pc, 
			Map<String, UserTable> p) throws PEException {
		return null;
	}

	@Override
	public void verifySame(SchemaContext pc, Persistable<PESchema, Map<String,UserTable>> other) throws PEException {
	}
	
	@Override
	public Map<String,UserTable> persistTree(SchemaContext pc)	throws PEException {
		checkLoaded(pc);
		return super.persistTree(pc);
	}

	@Override
	protected Class<? extends CatalogEntity> getPersistentClass() {
		return null;
	}	
	
	public Collection<SchemaCacheKey<?>> getCascades() {
		return lookup.getCascades();
	}
	
	public PEDatabase getPEDatabase(SchemaContext sc) {
		if (pdb != null) {
			return pdb.get(sc);
		}
		
		return null;
	}
}
