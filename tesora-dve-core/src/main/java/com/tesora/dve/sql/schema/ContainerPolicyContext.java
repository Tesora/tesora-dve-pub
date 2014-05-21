// OS_STATUS: public
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.node.MultiEdge;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ContainerTenantIDLiteral;
import com.tesora.dve.sql.node.expression.Default;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.schema.cache.SchemaCacheKey;
import com.tesora.dve.sql.schema.cache.SchemaEdge;
import com.tesora.dve.sql.schema.mt.AdaptiveMultitenantSchemaPolicyContext;
import com.tesora.dve.sql.schema.mt.IPETenant;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.ddl.PECreateTableStatement;
import com.tesora.dve.sql.statement.dml.InsertIntoValuesStatement;
import com.tesora.dve.sql.statement.dml.InsertStatement;
import com.tesora.dve.sql.statement.dml.InsertIntoValuesStatement.ValueHandler;
import com.tesora.dve.sql.transform.execution.ExecutionType;
import com.tesora.dve.sql.util.Pair;

public class ContainerPolicyContext extends SchemaPolicyContext {

	SchemaEdge<PEContainerTenant> currentTenant;
	boolean currentIsGlobal;
	
	@SuppressWarnings("unchecked")
	protected ContainerPolicyContext(SchemaContext cntxt, PEContainerTenant ten) {
		super(cntxt);
		currentTenant = StructuralUtils.buildEdge(cntxt,ten, true);
		if (ten != null)
			currentIsGlobal = ten.isGlobalTenant();
	}

	@Override
	public boolean allowTenantColumnDeclaration() {
		return true;
	}

	@Override
	public Statement modifyCreateTable(PECreateTableStatement stmt) {
		PETable pet = stmt.getCreated().get();
		// for containers we only add the tenant column.
		addTenantColumn(pet, AdaptiveMultitenantSchemaPolicyContext.tenantColumnName);
		return stmt;
	}

	
	
	public static Statement modifyCreateTable(SchemaContext spc, PECreateTableStatement stmt) {
		return new ContainerPolicyContext(spc,null).modifyCreateTable(stmt);
	}
	
	@Override
	public Long getTenantID(boolean mustExist) {
		if (currentTenant.get(getSchemaContext()).isGlobalTenant()) {
			if (mustExist)
				throw new SchemaException(Pass.PLANNER, "Global container context is not a tenant");
			return null;
		} else {
			return currentTenant.get(getSchemaContext()).getTenantID();
		}			
	}

	@Override
	public boolean isCacheableInsert(InsertIntoValuesStatement stmt) {
		PEAbstractTable<?> pet = stmt.getTableInstance().getAbstractTable();
		if (pet.isContainerBaseTable(getSchemaContext())) {
			if (currentTenant.get(getSchemaContext()) == null ||
					!currentTenant.get(getSchemaContext()).isGlobalTenant()) 
				throw new SchemaException(Pass.NORMALIZE, "Inserts into base table " 
						+ pet.getName().getSQL() 
						+ " for container " + pet.getDistributionVector(getSchemaContext()).getContainer(getSchemaContext()).getName().getSQL()
						+ " must be done when in the global container context");
			// container base table is never cacheable due to the implicit ddl
			return false;
		} else if (pet.getDistributionVector(getSchemaContext()).getContainer(getSchemaContext()) != null) {
			// for container tables the context must be set, and must not be the global context
			// we do this so that we can figure out what the tenant id should be
			if (currentTenant.get(getSchemaContext()) == null ||
					currentTenant.get(getSchemaContext()).isGlobalTenant())
				throw new SchemaException(Pass.NORMALIZE, "Inserts into table "
						+ pet.getName().getSQL()
						+ " for container " + pet.getDistributionVector(getSchemaContext()).getContainer(getSchemaContext()).getName().getSQL()
						+ " must be done when in a specific container context");
		}
		return true;
	}
	
	@Override
	public ValueHandler handleTenantColumnUponInsert(InsertIntoValuesStatement stmt, PEColumn column) {
		PEContainer container = null;
		container = column.getTable().getDistributionVector(getSchemaContext()).getContainer(getSchemaContext());
		boolean isBaseTable = column.getTable().isContainerBaseTable(getSchemaContext()); 
		return new ContainerTenantColumnHandler(this, container, stmt,column, isBaseTable);
	}

	@Override
	public boolean isDataTenant() {
		return !currentIsGlobal;
	}
	
	@Override
	public boolean isContainerContext() {
		return true;
	}
	
	@Override
	public IPETenant getCurrentTenant() {
		return currentTenant.get(getSchemaContext());
	}
	
	@Override
	public boolean requiresMTRewrites(ExecutionType et) {
		if (ExecutionType.INSERT == et) {
			return true;
		} else if (currentIsGlobal) {
			return false;
		} else {
			return true;
		}
	}

	@Override
	public boolean showTenantColumn() {
		return false;
	}
	
	@Override
	public boolean isAlwaysDistKey() {
		return !currentIsGlobal;
	}
	
	@SuppressWarnings("unchecked")
	public LiteralExpression getTenantIDLiteral(boolean mustExist, PEContainer cont) {
		Long value = getTenantID(mustExist);
		if (value == null) return null;
		sc.getValueManager().setTenantID(sc,value);
		return new ContainerTenantIDLiteral(sc.getValueManager(), (SchemaCacheKey<PEContainer>) cont.getCacheKey());
	}

	
	private Long allocateNewTenant(PEContainer cont, String discValue) {
		PEContainerTenant existing = getSchemaContext().findContainerTenant(cont, discValue);
		if (existing != null)
			return existing.getTenantID();
		getSchemaContext().getCatalog().saveContainerTenant(cont.getPersistent(getSchemaContext()), discValue);
		existing = getSchemaContext().findContainerTenant(cont, discValue);
		return existing.getTenantID();
	}
	
	private static class ContainerTenantColumnHandler extends ValueHandler {

		private ContainerPolicyContext context;
		private PEContainer container;
		private boolean baseTable;
		
		public ContainerTenantColumnHandler(ContainerPolicyContext me, PEContainer container, InsertIntoValuesStatement stmt, PEColumn col, boolean isBaseTable) {
			super(stmt, col);
			this.container = container;
			context = me;
			baseTable = isBaseTable;
		}

		@Override
		public ExpressionNode handle(SchemaContext sc, SQLMode mode, MultiEdge<InsertStatement,ExpressionNode> spec, 
				MultiEdge<InsertIntoValuesStatement,ExpressionNode> existing, 
				List<ExpressionNode> prior, ExpressionNode v) {
			boolean missing = false;
			if (v instanceof Default || v == null) 
				missing = true;
			if (!missing) return v;
			if (!baseTable) {
				// not the base table, get the the tenant id literal
				return context.getTenantIDLiteral(true, container);
			} else {
				// allocate a new tenant
				HashMap<PEColumn,LiteralExpression> values = new HashMap<PEColumn,LiteralExpression>();
				List<PEColumn> discriminatorColumns = container.getDiscriminantColumns(sc);
				for(int i = 0; i < spec.size(); i++) {
					ColumnInstance ci = (ColumnInstance) spec.get(i);
					if (discriminatorColumns.contains(ci.getPEColumn())) {
						ExpressionNode en = null;
						if (i < existing.size())
							en = existing.get(i);
						else
							en = prior.get(i - existing.size());
						values.put(ci.getPEColumn(), (LiteralExpression)en);
					}
				}
				List<Pair<PEColumn,LiteralExpression>> ordered = new ArrayList<Pair<PEColumn,LiteralExpression>>();
				for(PEColumn pec : discriminatorColumns) {
					ordered.add(new Pair<PEColumn,LiteralExpression>(pec,values.get(pec)));
				}
				String disc = PEContainerTenant.buildDiscriminantValue(sc, ordered);
				Long nv = context.allocateNewTenant(container,disc);
				return LiteralExpression.makeLongLiteral(nv.longValue());
			}
		}

		
		@Override
		public ExpressionNode handleMissing(SchemaContext sc) {
			throw new SchemaException(Pass.NORMALIZE, "Invalid call to container tenant column handler");
		}
		
	}
	
}
