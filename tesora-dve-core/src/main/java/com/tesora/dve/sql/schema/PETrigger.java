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
import com.tesora.dve.sql.schema.PEAbstractTable.TableCacheKey;
import com.tesora.dve.sql.schema.cache.CacheSegment;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.schema.cache.SchemaEdge;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.StatementType;

public class PETrigger extends Persistable<PETrigger, UserTrigger> {

	private final SchemaEdge<PETable> triggerTable;
	private final StatementType triggerType;
	private final Statement body;
	private SchemaEdge<PEUser> definer;
	
	private String collationConnection;
	private String charsetConnection;
	private String collationDatabase;
	
	public PETrigger(SchemaContext sc, Name name, PETable targetTable, Statement body, StatementType triggerOn,
			PEUser user, String collationConnection, String charsetConnection, String collationDatabase) {
		super(buildCacheKey(name,targetTable));
		setName(name.getUnqualified());
		this.body = body;
		this.triggerTable = StructuralUtils.buildEdge(sc, targetTable, false);
		this.triggerType = triggerOn;
		this.definer = StructuralUtils.buildEdge(sc,user,false);
		setPersistent(sc,null,null);
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected UserTrigger createEmptyNew(SchemaContext sc)
			throws PEException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected void populateNew(SchemaContext sc, UserTrigger p)
			throws PEException {
		// TODO Auto-generated method stub
		
	}

	@Override
	protected Persistable<PETrigger, UserTrigger> load(SchemaContext sc,
			UserTrigger p) throws PEException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected String getDiffTag() {
		// TODO Auto-generated method stub
		return null;
	}

	
	public static TriggerCacheKey buildCacheKey(Name trgName, PETable onTab) {
		return new TriggerCacheKey((TableCacheKey) onTab.getCacheKey(),trgName);
	}
	
	protected static class TriggerCacheKey extends SchemaCacheKey<PETrigger> {

		private final TableCacheKey targetTable;
		private final Name triggerName;
		
		public TriggerCacheKey(TableCacheKey onTable, Name trgName) {
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
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public String toString() {
			return String.format("PETrigger:%s(%s)",triggerName.getUnquotedName().get(),targetTable.toString());
		}
		
	}
}
