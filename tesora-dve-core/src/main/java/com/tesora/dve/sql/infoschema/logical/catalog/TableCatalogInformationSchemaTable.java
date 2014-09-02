package com.tesora.dve.sql.infoschema.logical.catalog;

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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.sql.expression.ExpressionPath;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.infoschema.CatalogLogicalInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.InformationSchemaException;
import com.tesora.dve.sql.infoschema.LogicalInformationSchema;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaTable;
import com.tesora.dve.sql.infoschema.engine.LogicalCatalogQuery;
import com.tesora.dve.sql.infoschema.engine.NamedParameter;
import com.tesora.dve.sql.infoschema.engine.ScopedColumnInstance;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.util.ListSet;

public class TableCatalogInformationSchemaTable extends
		CatalogInformationSchemaTable {

	protected CatalogInformationSchemaTable visibilityTable;
	protected CatalogInformationSchemaTable tenantTable;
	protected TableNameColumn nameColumn;
	protected CatalogLogicalInformationSchemaColumn visibilityLocalName;
	protected CatalogLogicalInformationSchemaColumn visibilityTableRef;
	protected CatalogLogicalInformationSchemaColumn tenantColumn;
	protected CatalogLogicalInformationSchemaColumn tenantIDColumn;
	protected CatalogLogicalInformationSchemaColumn tableStateColumn;
	
	public TableCatalogInformationSchemaTable(Class<?> entKlass,
			InfoSchemaTable anno, String catTabName) {
		super(entKlass, anno, catTabName);
		visibilityTable = null;
		nameColumn = null;
	}

	@Override
	protected void prepare(LogicalInformationSchema schema, DBNative dbn) {
		visibilityTable = (CatalogInformationSchemaTable) schema.lookup("table_visibility");
		tenantTable = (CatalogInformationSchemaTable) schema.lookup("tenant");
		visibilityLocalName = (CatalogLogicalInformationSchemaColumn) visibilityTable.lookup("local_name");
		visibilityTableRef = (CatalogLogicalInformationSchemaColumn) visibilityTable.lookup("user_table");
		tenantColumn = (CatalogLogicalInformationSchemaColumn) visibilityTable.lookup("tenant");
		tenantIDColumn = (CatalogLogicalInformationSchemaColumn) tenantTable.lookup("id");
		tableStateColumn = (CatalogLogicalInformationSchemaColumn) lookup("state");
		if (nameColumn == null)
			throw new InformationSchemaException("Unable to find name column on table");
		super.prepare(schema, dbn);
	}
	
	@Override
	public LogicalInformationSchemaColumn addColumn(SchemaContext sc, LogicalInformationSchemaColumn c) {
		LogicalInformationSchemaColumn actual = c;
		if ("name".equals(c.getName().get())) {
			nameColumn = new TableNameColumn((CatalogLogicalInformationSchemaColumn)c); 
			actual = nameColumn; 
		}
		super.addColumn(sc, actual);
		return actual;
	}
	
	@Override
	public boolean isLayered() {
		return true;
	}
	
	@Override
	public LogicalCatalogQuery explode(SchemaContext sc, LogicalCatalogQuery lq) {
		if (!sc.hasCurrentDatabase() || sc.getCurrentDatabase().isInfoSchema()) return lq;
		
		boolean tenant = sc.getPolicyContext().isSchemaTenant();

		SelectStatement in = lq.getQuery();

		if (tenant) {
		
			TableInstance origti = null;
			for(Iterator<FromTableReference> iter = in.getTablesEdge().iterator(); iter.hasNext();) {
				FromTableReference ftr = iter.next();
				if (ftr.getBaseTable() != null && ftr.getBaseTable().getTable() == this) {
					origti = ftr.getBaseTable();
					if (!ftr.getTableJoins().isEmpty())
						throw new InformationSchemaException("No support for joins against table");
					iter.remove();
					break;
				}
			}
			TableInstance nti = new TableInstance(visibilityTable,null,origti.getAlias(),origti.getNode(),false);
			in.getTablesEdge().add(new FromTableReference(nti));
			in.getDerivedInfo().addLocalTable(nti.getTableKey());
			// we're going to mod the name test in the where clause now
			if (in.getWhereClause() != null) {
				ExpressionNode out = buildTenantTableNameTest(in.getWhereClause(),nameColumn,
						nti,visibilityLocalName,visibilityTableRef);
				in.setWhereClause(out);
			}
			// add the tenant id test
			List<ExpressionNode> decompAnd = ExpressionUtils.decomposeAndClause(in.getWhereClause());
			ColumnInstance tenantRef = new ColumnInstance(tenantColumn, nti);
			ScopedColumnInstance idRef = new ScopedColumnInstance(tenantIDColumn, tenantRef);
			FunctionCall tenantFilter = new FunctionCall(FunctionName.makeEquals(),idRef,new NamedParameter(new UnqualifiedName("tenantID")));
			lq.getParams().put("tenantID", sc.getPolicyContext().getTenantID(true).intValue());
			decompAnd.add(tenantFilter);
			in.setWhereClause(ExpressionUtils.safeBuildAnd(decompAnd));

			// forward the table references
			ColumnInstance tabRef = new ColumnInstance(visibilityTableRef, nti);
			new LogicalInformationSchemaTable.PathExtender(origti,tabRef).traverse(in);

			TableKey found = null;
			for(Map.Entry<TableKey, ExpressionNode> me : lq.getForwarding().entrySet()) {
				if (me.getValue() instanceof TableInstance) {
					TableInstance ti = (TableInstance)me.getValue();
					if (ti.getTable() == this) {
						found = me.getKey();
					}
				}
			}
			if (found == null)
				throw new InformationSchemaException("Cannot find replaced table");
			lq.getForwarding().remove(found);
			lq.getForwarding().put(found, (ExpressionNode) tabRef.copy(null));

			return new LogicalCatalogQuery(lq,in);
		}
		return lq;
	}

	
	private static class TableNameColumn extends CatalogLogicalInformationSchemaColumn {

		public TableNameColumn(CatalogLogicalInformationSchemaColumn given) {
			super(given);
		}

		@Override
		protected Object getRawValue(SchemaContext sc, CatalogEntity ce) {
			UserTable tab = (UserTable) ce;
			String result = tab.getName();
			if (sc.getPolicyContext().isSchemaTenant()) {
				result = sc.getPolicyContext().getLocalName(tab).get();
			}
			return result;
		}
		
	}

	public static ExpressionNode buildTenantTableNameTest(ExpressionNode root, 
			LogicalInformationSchemaColumn tableNameColumn,
			TableInstance tvti,
			LogicalInformationSchemaColumn tvLocalNameColumn,
			LogicalInformationSchemaColumn tvUserTableColumn) {
		ListSet<ColumnInstance> nameRefs = 
				LogicalInformationSchemaTable.NameRefCollector.collect(root, tableNameColumn);
		HashMap<ColumnInstance, ExpressionNode> suitable = new HashMap<ColumnInstance,ExpressionNode>();
		for(ColumnInstance ci : nameRefs) {
			// find a convenient subtree around the name test
			List<ExpressionNode> candidates = new ArrayList<ExpressionNode>();
			LanguageNode cn = ci.getParent();
			while(cn != root) {
				candidates.add((ExpressionNode) cn);
				cn = cn.getParent();
			}
			int candindex = -2;
			for(int i = 0; i < candidates.size(); i++) {
				ExpressionNode en = candidates.get(i);
				if (EngineConstant.FUNCTION.has(en)) {
					FunctionCall fc = (FunctionCall) en;
					if (fc.getFunctionName().isBooleanOperator()) {
						candindex = i - 1;
						break;
					}
				}
			}
			ExpressionNode candidate = null;
			if (candindex == -2)
				candidate = root;
			else
				candidate = candidates.get(candindex);
			suitable.put(ci, candidate);
		}
		ExpressionNode out = root;
		for(Map.Entry<ColumnInstance, ExpressionNode> me : suitable.entrySet()) {
			ExpressionPath inep = ExpressionPath.build(me.getKey(), me.getValue());
			ExpressionNode dup1 = (ExpressionNode) me.getValue().copy(null);
			ExpressionNode dup2 = (ExpressionNode) me.getValue().copy(null);
			ColumnInstance ln = new ColumnInstance(tvLocalNameColumn, tvti);
			ColumnInstance tabRef = new ColumnInstance(tvUserTableColumn, tvti);
			ScopedColumnInstance sci = new ScopedColumnInstance(tableNameColumn,tabRef);
			inep.update(dup1, ln);
			inep.update(dup2, sci);
			dup1.setGrouped();
			dup2.setGrouped();
			FunctionCall orCall = new FunctionCall(FunctionName.makeOr(),dup1,dup2);
			orCall.setGrouped();
			Edge<?,ExpressionNode> pedge = me.getValue().getParentEdge();
			pedge.set(orCall);
			if (me.getValue() == root)
				out = orCall;
		}
		return out;
	}
	
}
