// OS_STATUS: public
package com.tesora.dve.sql.schema.mt;

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

import java.util.ArrayList;
import java.util.Collection;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.TableVisibility;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.lockmanager.LockSpecification;
import com.tesora.dve.sql.schema.HasName;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.PETable;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.StructuralUtils;
import com.tesora.dve.sql.schema.TableUseLock;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.PEAbstractTable.TableCacheKey;
import com.tesora.dve.sql.schema.cache.CacheSegment;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.schema.cache.SchemaEdge;
import com.tesora.dve.sql.schema.mt.PETenant.TenantCacheKey;

public class TableScope extends Persistable<TableScope, TableVisibility> implements HasName {

	protected SchemaEdge<PETenant> tenant;
	protected SchemaEdge<PEAbstractTable<?>> table;
	protected Long autoIncrStart;
	protected Integer autoIncID;
	
	@SuppressWarnings("unchecked")
	public TableScope(SchemaContext sc, PETable tab, PETenant ten, Long autoIncrFirstValue, Name name) {
		super(getScopeKey(ten,name));
		tenant = StructuralUtils.buildEdge(sc,ten, false);
		table = StructuralUtils.buildEdge(sc,tab,false);
		if (name != null)
			setName(name);
		else
			setName(tab.getName());
		autoIncrStart = autoIncrFirstValue;
	}

	public String toString(SchemaContext sc) {
		StringBuilder buf = new StringBuilder();
		buf.append("scope{tenant=").append(tenant.get(sc).getName())
			.append(",name=").append(getName().getSQL())
			.append(",table=").append(table.get(sc).getName().getSQL());
		buf.append("}");
		return buf.toString();		
	}
	
	@Override
	public String toString() {
		return toString(SchemaContext.threadContext.get());
	}
	
	public PETable getTable(SchemaContext sc) {
		return (PETable) table.get(sc);
	}
	
	public SchemaCacheKey<PEAbstractTable<?>> getTableKey() {
		return table.getCacheKey();
	}
	
	public PETenant getTenant(SchemaContext sc) {
		return tenant.get(sc);
	}

	public TableUseLock getLock(String why, SchemaContext sc) {
		return new TableUseLock(why,tenant.get(sc).getCacheKey().toString(),
				getName().getUnquotedName().get());
	}
	
	@SuppressWarnings("unchecked")
	public void setTable(SchemaContext sc, PETable pet) {
		table = StructuralUtils.buildEdge(sc,pet, false);
	}
	
	public static TableScope load(TableVisibility tv, SchemaContext sc) {
		TableScope ts = (TableScope)sc.getLoaded(tv,getScopeKey(tv));
		if (ts == null)
			ts = new TableScope(tv, sc);
		return ts;
	}
	
	@SuppressWarnings("unchecked")
	private TableScope(TableVisibility tv, SchemaContext sc) {
		super(getScopeKey(tv));
		sc.startLoading(this, tv);
		tenant = StructuralUtils.buildEdge(sc,PETenant.load(tv.getTenant(), sc),true);
		table = StructuralUtils.buildEdge(sc,PETable.load(tv.getTable(), sc),true);
		if (tv.getLocalName() != null)
			setName(new UnqualifiedName(tv.getLocalName()));
		else
			setName(table.get(sc).getName());
		if (tv.hasAutoIncr())
			autoIncID = tv.getAutoIncr().getId();
		setPersistent(sc,tv,tv.getId());
		sc.finishedLoading(this, tv);
	}

	public static TableScope load(HollowTableVisibility htv, SchemaContext sc) {
		TableScope ts = (TableScope)sc.getSource().getLoaded(buildRawKey(htv));
		if (ts == null)
			ts = new TableScope(htv,sc);
		return ts;		
	}
	
	private static ScopeCacheKey buildRawKey(HollowTableVisibility htv) {
		return new ScopeCacheKey(new TenantCacheKey(htv.getTenantName(),htv.getTenantID()),htv.getLocalName());
	}
	
	private TableScope(HollowTableVisibility htv, SchemaContext sc) {
		super(buildRawKey(htv));
		setName(new UnqualifiedName(htv.getLocalName()));
		ScopeCacheKey sck = (ScopeCacheKey) getCacheKey();
		SchemaCacheKey<PETenant> tck = sck.getTenantCacheKey();
		tenant = sc.buildEdgeFromKey(tck, true);
		SchemaCacheKey<PEAbstractTable<?>> backingTable = new TableCacheKey(htv.getDbid(), htv.getDbName(), new UnqualifiedName(htv.getTabName()));
		table = sc.buildEdgeFromKey(backingTable, true);
		autoIncID = htv.getAutoIncrementId();
		persistentID = htv.getScopeID();
		sc.getSource().setLoaded(this, sc.isCacheLoading(sck) ? null : sck);
	}
	
	@Override
	protected Class<? extends CatalogEntity> getPersistentClass() {
		return TableVisibility.class;
	}

	@Override
	protected TableVisibility lookup(SchemaContext pc) throws PEException {
		Integer tabID = table.get(pc).getPersistentID();
		Integer tenID = tenant.get(pc).getPersistentID();
		if (tenID == null || tabID == null) return null;
		return pc.getCatalog().findVisibilityRecord(tabID.intValue(), tenID.intValue());
	}

	@Override
	protected TableVisibility createEmptyNew(SchemaContext pc) throws PEException {
		String name = (getName().equals(table.get(pc).getName()) ? null : getName().get());
		return new TableVisibility(tenant.get(pc).persistTree(pc), table.get(pc).persistTree(pc), name, autoIncrStart);
	}

	@Override
	protected void populateNew(SchemaContext pc, TableVisibility p) throws PEException {
	}

	@Override
	protected void updateExisting(SchemaContext pc, TableVisibility tv) throws PEException {
		tv.setTable(table.get(pc).persistTree(pc));
	}
	
	@Override
	protected Persistable<TableScope, TableVisibility> load(SchemaContext pc, TableVisibility p)
			throws PEException {
		return new TableScope(p, pc);
	}

	@Override
	protected int getID(TableVisibility p) {
		return p.getId();
	}

	@Override
	public Persistable<TableScope, TableVisibility> reload(SchemaContext usingContext) throws PEException {
		TableVisibility tv = lookup(usingContext);
		return new TableScope(tv, usingContext);
	}

	@Override
	protected String getDiffTag() {
		return "TableScope";
	}

	public static ScopeCacheKey getScopeKey(PETenant onTenant, Name onName) {
		return getScopeKey((TenantCacheKey) onTenant.getCacheKey(), onName.getUnquotedName().getUnqualified().get());
	}
	
	public static ScopeCacheKey getScopeKey(TenantCacheKey tenantKey, String name) {
		return new ScopeCacheKey(tenantKey,name);
	}
	
	public static SchemaCacheKey<TableScope> getScopeKey(TableVisibility tv) {
		return getScopeKey((TenantCacheKey)PETenant.getTenantKey(tv.getTenant()),(tv.getLocalName() != null ? tv.getLocalName() : tv.getTable().getName()));
	}
	
	public static class ScopeCacheKey extends SchemaCacheKey<TableScope> {

		private static final long serialVersionUID = 1L;
		private final TenantCacheKey tenantKey;
		private String name;
		
		public ScopeCacheKey(TenantCacheKey tenKey, String forName) {
			super();
			this.tenantKey = tenKey;
			this.name = forName;
		}
		
		@Override
		public int hashCode() {
			return addHash(initHash(TableScope.class,tenantKey.getTenantName().hashCode()),name.hashCode());
		}
		
		@Override
		public String toString() {
			return "TableScope:" + tenantKey.getTenantName() + "/" + name;
		}
		
		@Override
		public boolean equals(Object o) {
			if (o instanceof ScopeCacheKey) {
				ScopeCacheKey sck = (ScopeCacheKey) o;
				return this.tenantKey.equals(sck.tenantKey) && this.name.equals(sck.name);
			}
			return false;
		}

		@Override
		public TableScope load(SchemaContext sc) {
			// if it's not a persistent catalog we have to get the whole object
			// likewise if it's a mutable source we're doing ddl - need the whole object
			if (sc.isMutableSource() || !sc.getCatalog().isPersistent()) {
				TableVisibility tv = sc.getCatalog().findTableVisibility(tenantKey.getTenantName(), tenantKey.getTenantID(), name); 
				if (tv == null) return null;
				return TableScope.load(tv, sc);
			} else {
				HollowTableVisibility htv = sc.getCatalog().findHollowTableVisibility(tenantKey.getTenantName(), tenantKey.getTenantID(), name);
				if (htv == null) return null;
				return TableScope.load(htv, sc);
			}
		}
		
		@Override
		public CacheSegment getCacheSegment() {
			return CacheSegment.SCOPE;
		}

		public SchemaCacheKey<PETenant> getTenantCacheKey() {
			return tenantKey;
		}
		
		@Override
		public Collection<SchemaCacheKey<?>> getCascades(Object obj) {
			TableScope ts = (TableScope) obj;
			ArrayList<SchemaCacheKey<?>> others = new ArrayList<SchemaCacheKey<?>>();
			others.add(ts.table.getCacheKey());
			return others;
		}

		@Override
		public SchemaCacheKey<?> getEnclosing() {
			return tenantKey;
		}

		@Override
		public LockSpecification getLockSpecification(String why) {
			return new TableUseLock(why,tenantKey.toString(),name);
		}
		
	}

	public Integer getAutoIncrementID() {
		return autoIncID;
	}
	
	public static boolean valid(TableVisibility tv) {
		if (tv.getTable() == null)
			return false;
		if (tv.getTenant() == null)
			return false;
		return PETable.valid(tv.getTable());
	}
}
