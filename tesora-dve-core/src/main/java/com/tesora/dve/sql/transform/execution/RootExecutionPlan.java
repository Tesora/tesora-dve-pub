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



import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepOperation;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.parser.ExtractedLiteral;
import com.tesora.dve.sql.schema.ConnectionValues;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.ValueManager;
import com.tesora.dve.sql.schema.cache.CachedPlan;
import com.tesora.dve.sql.statement.StatementType;
import com.tesora.dve.sql.util.UnaryProcedure;

public class RootExecutionPlan extends ExecutionPlan {

	private ProjectionInfo projection;
	// cacheable only if true
	private Boolean cacheable;
	@SuppressWarnings("unused")
	private CachedPlan owner;
	
	
	// com_* stats are populated via this when there is a plan cache hit
	private final StatementType originalStatementType;

	private boolean isEmptyPlan = false;
	
	public RootExecutionPlan(ProjectionInfo pi, ValueManager vm, StatementType stmtType) {
		super(vm);
		projection = pi;
		originalStatementType = stmtType;
	}
	
	public List<QueryStepOperation> schedule(ExecutionPlanOptions opts, SSConnection connection, SchemaContext sc, ConnectionValuesMap cv) throws PEException {
		List<QueryStepOperation> buf = new ArrayList<QueryStepOperation>();
		schedule(opts, buf,projection,sc, cv, (ExecutionPlan)this);
		Long lastInsertId = steps.getlastInsertId(values,sc,cv.getValues(this));
		if (lastInsertId != null)
			connection.setLastInsertedId(lastInsertId.longValue());
		else 
			connection.setLastInsertedId(0);
		return buf;
	}

	
	@Override
	public void setCacheable(boolean v) {
		// i.e. peg not cacheable
		if (Boolean.FALSE.equals(cacheable)) return;
		else if (!v) cacheable = false;
		else cacheable = v;
	}
	
	@Override
	public boolean isCacheable() {
		return Boolean.TRUE.equals(cacheable);
	}

	public void setOwningCache(CachedPlan cp) {
		owner = cp;
		values.setFrozen();
		prepareForCache();
	}
	
	public ProjectionInfo getProjectionInfo() {
		return projection;
	}
	
	public StatementType getStatementType() {
		return originalStatementType;
	}

	public boolean isEmptyPlan() {
		return isEmptyPlan;
	}

	public void setIsEmptyPlan(boolean isEmptyPlan) {
		this.isEmptyPlan = isEmptyPlan;
	}


	@Override
	public boolean isRoot() {
		return true;
	}

	// this does planning time resets
	public ConnectionValuesMap resetForNewPlan(final SchemaContext sc, final List<ExtractedLiteral> literalValues) throws PEException {
		final ConnectionValuesMap cvm = new ConnectionValuesMap();
		traverseExecutionPlans(new UnaryProcedure<ExecutionPlan>() {

			@Override
			public void execute(ExecutionPlan object) {
				try {
					if (object.isRoot()) {
						cvm.addValues(object, object.getValueManager().resetForNewPlan(sc, literalValues));
					} else {
						cvm.addValues(object, object.getValueManager().resetForNewNestedPlan(sc));
					}
				} catch (PEException pe) {
					throw new SchemaException(Pass.PLANNER, pe);
				}
			}
			
		});
		return cvm;
	}
	
	public ConnectionValuesMap resetForNewPStmtExec(final SchemaContext sc, final List<Object> params) throws PEException {
		final ConnectionValuesMap cvm = new ConnectionValuesMap();
		traverseExecutionPlans(new UnaryProcedure<ExecutionPlan>() {

			@Override
			public void execute(ExecutionPlan object) {
				try {
					if (object.isRoot()) {
						cvm.addValues(object, object.getValueManager().resetForNewPStmtExec(sc, params));
					} else {
						cvm.addValues(object, object.getValueManager().resetForNewNestedPlan(sc));
					}
				} catch (PEException pe) {
					throw new SchemaException(Pass.PLANNER,pe);
				}
			}
			
		});
		return cvm;
	}
	
	public void collectNonRootValueTemplates(final SchemaContext sc, final ConnectionValuesMap cvm) {
		traverseExecutionPlans(new UnaryProcedure<ExecutionPlan>() {

			@Override
			public void execute(ExecutionPlan object) {
				if (object.isRoot()) return;
				try {
					cvm.addValues(object, object.getValueManager().resetForNewNestedPlan(sc));
				} catch (PEException pe) {
					throw new SchemaException(Pass.PLANNER,pe);
				}
			}
			
		});
	}
}
