// OS_STATUS: public
package com.tesora.dve.sql.transform.execution;

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

import java.util.List;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStep;
import com.tesora.dve.queryplan.QueryStepAlterDatabaseOperation;
import com.tesora.dve.queryplan.QueryStepCreateDatabaseOperation;
import com.tesora.dve.queryplan.QueryStepCreateUserOperation;
import com.tesora.dve.queryplan.QueryStepDDLOperation;
import com.tesora.dve.queryplan.QueryStepDropDatabaseOperation;
import com.tesora.dve.queryplan.QueryStepGrantPrivilegesOperation;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PEPriviledge;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.PEUser;
import com.tesora.dve.sql.schema.Persistable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.sql.schema.cache.InvalidationScope;
import com.tesora.dve.sql.util.Functional;

public class SimpleDDLExecutionStep extends CatalogModificationExecutionStep {

	protected List<CatalogEntity> deletes;
	protected List<CatalogEntity> updates;
	
	protected SQLCommand sql;
	private Boolean commitOverride = null;
	
	protected CacheInvalidationRecord invalidationRecord;

	public SimpleDDLExecutionStep(PEDatabase db, PEStorageGroup tsg,
			Persistable<?, ?> root, Action act, SQLCommand sql, List<CatalogEntity> deleteList,
			List<CatalogEntity> entityList,
			CacheInvalidationRecord invalidate) {
		super(db, tsg, root, act);
		this.deletes = deleteList;
		this.updates = entityList;
		this.invalidationRecord = invalidate;
		this.sql = sql;
	}

	public SimpleDDLExecutionStep withCommitOverride(boolean v) {
		setCommitOverride(v);
		return this;
	}
	
	public List<CatalogEntity> getEntities() {
		return updates;
	}
	
	public List<CatalogEntity> getDeletes() {
		return deletes;
	}
	
	protected QueryStepDDLOperation buildOperation(SchemaContext sc) throws PEException {
		QueryStepDDLOperation qso = new QueryStepDDLOperation(getPersistentDatabase(), sql,getCacheInvalidation(sc));
		if (getCommitOverride() != null)
			return (QueryStepDDLOperation) qso.withCommitOverride(getCommitOverride());
		return qso;
	}

	@Override
	public void schedule(ExecutionPlanOptions opts, List<QueryStep> qsteps, ProjectionInfo projection, SchemaContext sc)
			throws PEException {
		if (rootEntity instanceof PEDatabase) {
			PEDatabase db = (PEDatabase)rootEntity;
			CacheInvalidationRecord invalidate = new CacheInvalidationRecord(db.getCacheKey(),
					(((action == Action.DROP) || (action == Action.ALTER)) ? InvalidationScope.CASCADE : InvalidationScope.LOCAL));
			if (action == Action.CREATE) {
				addStep(sc,qsteps,new QueryStepCreateDatabaseOperation(db.getPersistent(sc),invalidate));
				return;
			} else if (action == Action.DROP) {
				addStep(sc,qsteps,new QueryStepDropDatabaseOperation(db.getPersistent(sc),invalidate));
				return;
			} else if (action == Action.ALTER) {
				addStep(sc, qsteps, new QueryStepAlterDatabaseOperation(db.getPersistent(sc), invalidate));
				return;
			}
		} else if (rootEntity instanceof PEUser) {
			PEUser peu = (PEUser) rootEntity;
			CacheInvalidationRecord invalidate = new CacheInvalidationRecord(peu.getCacheKey(),InvalidationScope.LOCAL);
			if (action == Action.CREATE) {
				addStep(sc,qsteps,new QueryStepCreateUserOperation(peu.getPersistent(sc),invalidate));
				return;
			}
		} else if (rootEntity instanceof PEPriviledge) {
			PEPriviledge priv = (PEPriviledge)rootEntity;
			if (action == Action.ALTER) {
				// if the priviledge is a global priviledge, we can just do a regular ddl operation
				// but if it is for a specific database/tenant, we have to do a grant priv operation
				if (!priv.isGlobal())
					addStep(sc,qsteps,new QueryStepGrantPrivilegesOperation(priv.getPersistent(sc),getCacheInvalidation(sc)));
			}
		}
		QueryStepDDLOperation qso = buildOperation(sc);
		for(CatalogEntity ce : getEntities()) {
			qso.addCatalogUpdate(ce);
		}
		for(CatalogEntity ce : getDeletes()) {
			qso.addCatalogDeletion(ce);
		}
		addStep(sc,qsteps,qso);
	}
		
	@Override
	public void display(SchemaContext sc, List<String> buf, String indent, EmitOptions opts) {
		super.display(sc, buf, indent, opts);
		// drops might not have a root entity
		if (rootEntity == null)
			buf.add(indent + "  DDL " + action.name());
		else {
			buf.add(indent + "  DDL " + action.name() + " on " + rootEntity.getName() + 
					" (type " + rootEntity.getClass().getName() + "): " + rootEntity);
			buf.add(indent + "updates: {" + Functional.joinToString(getEntities(), ",") + "}");
			buf.add(indent + "deletes: {" + Functional.joinToString(getDeletes(), ",") + "}");
		}
	}

	@Override
	public void getSQL(SchemaContext sc, List<String> buf, EmitOptions opts) {
		buf.add(sql.getRawSQL());
	}

	@Override
	public CacheInvalidationRecord getCacheInvalidation(SchemaContext sc) {
		return invalidationRecord;
	}

	public Boolean getCommitOverride() {
		return commitOverride;
	}

	private void setCommitOverride(Boolean commitOverride) {
		this.commitOverride = commitOverride;
	}
}
