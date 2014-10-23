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

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.UserTrigger;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.parser.TranslatorInitCallback;
import com.tesora.dve.sql.parser.TranslatorUtils;
import com.tesora.dve.sql.schema.PEAbstractTable.TableCacheKey;
import com.tesora.dve.sql.schema.cache.CacheSegment;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.schema.cache.SchemaEdge;
import com.tesora.dve.sql.statement.Statement;

public class PETrigger extends Persistable<PETrigger, UserTrigger> {

	// triggers only make sense in the context of the target table, so no need for an edge
	// also, triggers are loaded/unloaded with the table
	private final PETable triggerTable;
	private final TriggerEvent triggerType;
	private String bodySrc;
	private String rawSQL;
	private SchemaEdge<PEUser> definer;
	
	private final boolean before;
	private final SQLMode sqlMode;
	
	private final String collationConnection;
	private final String charsetConnection;
	private final String collationDatabase;
	
	public PETrigger(SchemaContext sc, Name name, PETable targetTable, Statement body, TriggerEvent triggerOn,
			PEUser user, String collationConnection, String charsetConnection, String collationDatabase,
			boolean before, SQLMode sqlMode, String rawSQL) {
		super(buildCacheKey(name,targetTable));
		setName(name.getUnqualified());
		this.bodySrc = body.getSQL(sc);
		this.triggerTable = targetTable;
		this.triggerType = triggerOn;
		this.definer = StructuralUtils.buildEdge(sc,user,false);
		this.before = before;
		this.sqlMode = sqlMode;
		this.collationConnection = collationConnection;
		this.charsetConnection = charsetConnection;
		this.collationDatabase = collationDatabase;
		this.rawSQL = rawSQL;
		setPersistent(sc,null,null);
	}
	
	public PEUser getDefiner(SchemaContext sc) {
		return definer.get(sc);
	}
	
	public boolean isBefore() {
		return before;
	}
	
	public TriggerEvent getEvent() {
		return triggerType;
	}
	
	public PETable getTargetTable() {
		return triggerTable;
	}

	public String getBodySource() {
		return bodySrc;
	}
	
	public Statement getBody(SchemaContext sc) {
		Statement parsed = PEView.buildStatement(sc, triggerTable.getPEDatabase(sc), bodySrc, false, new ScopeInjector(triggerTable));
		return parsed;
	}
	
	public static PETrigger load(UserTrigger ut, SchemaContext sc, PETable onTable) {
		TriggerCacheKey tck = null;
		if (onTable == null)
			tck = buildCacheKey(ut.getName(),(TableCacheKey) PETable.getTableKey(ut.getTable()));
		else
			tck = buildCacheKey(ut.getName(), (TableCacheKey) onTable.getCacheKey());
		PETrigger pep = (PETrigger) sc.getLoaded(ut,tck);
		if (pep == null)
			pep = new PETrigger(ut,sc,onTable);
		return pep;
	}
	
	private PETrigger(UserTrigger ut, SchemaContext sc, PETable onTable) {
		super(buildCacheKey(ut.getName(),
				(TableCacheKey)(onTable == null ? PETable.getTableKey(ut.getTable()) : onTable.getCacheKey())));
		sc.startLoading(this, ut);
		setName(new UnqualifiedName(ut.getName()));
		setPersistent(sc,ut,ut.getId());
		PETable ttab = (onTable == null ? PETable.load(ut.getTable(), sc).asTable() : onTable);
		triggerTable = ttab;
		// do something with the body
		this.before = "BEFORE".equals(ut.getWhen());
		this.triggerType = TriggerEvent.valueOf(ut.getEvent());
		this.charsetConnection = ut.getCharsetConnection();
		this.collationConnection = ut.getCollationConnection();
		this.collationDatabase = ut.getDatabaseCollation();
		if ("".equals(ut.getSQLMode())) this.sqlMode = null;
		else this.sqlMode = new SQLMode(ut.getSQLMode());
		this.rawSQL = ut.getOrigSQL();
		this.bodySrc = ut.getBody();
		this.definer = StructuralUtils.buildEdge(sc,PEUser.load(ut.getDefiner(), sc),true);
	}
	
	public void setUser(SchemaContext sc, PEUser targ) {
		// transient because this should never be used on the persistent path
		this.definer = StructuralUtils.buildEdge(sc, targ, false);
	}
	
	@Override
	protected Class<? extends CatalogEntity> getPersistentClass() {
		return UserTrigger.class;
	}

	@Override
	protected int getID(UserTrigger p) {
		return p.getId();
	}

	@Override
	protected UserTrigger lookup(SchemaContext sc) throws PEException {
		return null;
	}

	@Override
	protected UserTrigger createEmptyNew(SchemaContext sc)
			throws PEException {
		return new UserTrigger(getName().get(),
				bodySrc,
				triggerTable.persistTree(sc),
				triggerType.name(),
				before ? "BEFORE" : "AFTER",
				(sqlMode == null ? "" : sqlMode.toString()),
				charsetConnection,
				collationConnection,
				collationDatabase,
				definer.get(sc).persistTree(sc),
				rawSQL);
	}

	@Override
	protected void populateNew(SchemaContext sc, UserTrigger p)
			throws PEException {
		// does nothing
	}

	@Override
	protected Persistable<PETrigger, UserTrigger> load(SchemaContext sc,
			UserTrigger p) throws PEException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected String getDiffTag() {
		return "trigger";
	}

	
	public static TriggerCacheKey buildCacheKey(String triggerName, TableCacheKey onTable) {
		return new TriggerCacheKey(onTable, triggerName);
	}
	
	public static TriggerCacheKey buildCacheKey(Name trgName, PETable onTab) {
		return new TriggerCacheKey((TableCacheKey) onTab.getCacheKey(),trgName.getUnqualified().getUnquotedName().get());
	}
	
	protected static class TriggerCacheKey extends SchemaCacheKey<PETrigger> {

		private final TableCacheKey targetTable;
		private final String triggerName;
		
		public TriggerCacheKey(TableCacheKey onTable, String trgName) {
			this.targetTable = onTable;
			this.triggerName = trgName;
		}
		
		
		// store triggers with tables
		public CacheSegment getCacheSegment() {
			return CacheSegment.TABLE;
		}
		
		@Override
		public int hashCode() {
			return addHash(initHash(PETrigger.class,triggerName.hashCode()),targetTable.hashCode());
		}

		@Override
		public boolean equals(Object o) {
			if (o instanceof TriggerCacheKey) {
				TriggerCacheKey otck = (TriggerCacheKey) o;
				return otck.targetTable.equals(targetTable) && otck.triggerName.equals(triggerName);
			}
			return false;
		}

		@Override
		public PETrigger load(SchemaContext sc) {
			UserTrigger ut = sc.getCatalog().findTrigger(triggerName, targetTable.getDatabaseName());
			if (ut == null)
				return null;
			return PETrigger.load(ut, sc, null);
		}

		@Override
		public String toString() {
			return String.format("PETrigger:%s(%s)",triggerName,targetTable.toString());
		}
		
	}
	
	private static class ScopeInjector extends TranslatorInitCallback {
		
		private final PETable target;
		
		public ScopeInjector(PETable targ) {
			this.target = targ;
		}
		
		public void onInit(TranslatorUtils utils) {
			utils.pushTriggerTable(target);
		}

	}
}
