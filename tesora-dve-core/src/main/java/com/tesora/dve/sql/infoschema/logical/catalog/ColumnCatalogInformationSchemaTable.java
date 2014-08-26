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

import java.util.List;

import com.tesora.dve.db.DBNative;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.infoschema.CatalogInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.InformationSchemaException;
import com.tesora.dve.sql.infoschema.LogicalInformationSchema;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaTable;
import com.tesora.dve.sql.infoschema.engine.LogicalCatalogQuery;
import com.tesora.dve.sql.infoschema.engine.NamedParameter;
import com.tesora.dve.sql.infoschema.engine.ScopedColumnInstance;
import com.tesora.dve.sql.node.expression.ActualLiteralExpression;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.mt.AdaptiveMultitenantSchemaPolicyContext;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.variables.KnownVariables;

public class ColumnCatalogInformationSchemaTable extends
		CatalogInformationSchemaTable {

	protected CatalogInformationSchemaColumn columnNameColumn;
	protected CatalogInformationSchemaColumn columnTableColumn;
	protected CatalogInformationSchemaColumn tableIDColumn;
	protected CatalogInformationSchemaColumn databaseIDColumn;
	protected CatalogInformationSchemaColumn databaseModeColumn;
	protected CatalogInformationSchemaTable scopeTable;
	protected CatalogInformationSchemaColumn scopeTenantColumn;
	protected CatalogInformationSchemaColumn scopeTableColumn;
	protected CatalogInformationSchemaColumn tableNameColumn;
	protected CatalogInformationSchemaColumn tenantID;
	protected CatalogInformationSchemaColumn localName;
	
	public ColumnCatalogInformationSchemaTable(Class<?> entKlass,
			InfoSchemaTable anno, String catTabName) {
		super(entKlass, anno, catTabName);
	}

	@Override
	protected void prepare(LogicalInformationSchema schema, DBNative dbn) {
		super.prepare(schema, dbn);
		CatalogInformationSchemaTable tableTable = (CatalogInformationSchemaTable) schema.lookup("table");
		columnNameColumn = (CatalogInformationSchemaColumn) lookup("name");
		columnTableColumn = (CatalogInformationSchemaColumn) lookup("user_table");
		tableIDColumn = (CatalogInformationSchemaColumn) tableTable.lookup("id");
		scopeTable = (CatalogInformationSchemaTable) schema.lookup("table_visibility");
		scopeTenantColumn = (CatalogInformationSchemaColumn) scopeTable.lookup("tenant");
		scopeTableColumn = (CatalogInformationSchemaColumn) scopeTable.lookup("user_table");
		tableNameColumn = (CatalogInformationSchemaColumn) tableTable.lookup("name");
		CatalogInformationSchemaTable tenantTable = (CatalogInformationSchemaTable) schema.lookup("tenant");
		tenantID = (CatalogInformationSchemaColumn) tenantTable.lookup("id");
		localName = (CatalogInformationSchemaColumn) scopeTable.lookup("local_name");
	}
	
	
	@Override
	public boolean isLayered() {
		return true;
	}
	
	@Override
	public LogicalCatalogQuery explode(SchemaContext sc, LogicalCatalogQuery lq) {
		if (sc.getCurrentDatabase(false) != null && sc.getCurrentDatabase().isInfoSchema()) return lq;

		// true when this is a regular schema tenant (not root)
		boolean schemaTenant = sc.getPolicyContext().isSchemaTenant();

		// when a tenant query, make sure we don't reveal the tenant column, and restrict to
		// columns in tables that are visible to this tenant
		
		SelectStatement in = lq.getQuery();
		ListSet<TableKey> tabs = EngineConstant.TABLES.getValue(in,sc);
		
		TableKey ours = null;
		for(TableKey tk : tabs) {
			if (tk.getTable() == this) {
				ours = tk;
				break;
			}
		}
		if (ours == null)
			throw new InformationSchemaException("Unable to find column table in column table query");
		FunctionCall joinUserScope = null;
		FunctionCall restrictTenant = null;
		
		// we only care about the scope table if this is a schema tenant
		if (schemaTenant) {
			// add our tenant specific filter working
			TableInstance tv = new TableInstance(scopeTable,null,in.getAliases().buildNewAlias(new UnqualifiedName("ts")),sc.getNextTable(),false);
			in.getTablesEdge().add(new FromTableReference(tv));
			in.getDerivedInfo().addLocalTable(tv.getTableKey());
			// uc.userTable.id = tv.table.id
			ColumnInstance userTable = new ColumnInstance(columnTableColumn,ours.toInstance());
			ScopedColumnInstance userTableID = new ScopedColumnInstance(tableIDColumn,userTable);
			ColumnInstance tvTable = new ColumnInstance(scopeTableColumn,tv);
			ScopedColumnInstance scopeTableID = new ScopedColumnInstance(tableIDColumn,tvTable);
			joinUserScope = new FunctionCall(FunctionName.makeEquals(),userTableID, scopeTableID);
			// tv.ofTenant.id = currentID
			ColumnInstance tvTenant = new ColumnInstance(scopeTenantColumn,tv);
			ScopedColumnInstance tvTenantID = new ScopedColumnInstance(tenantID,tvTenant);
			restrictTenant = new FunctionCall(FunctionName.makeEquals(),tvTenantID,new NamedParameter(new UnqualifiedName("tenantID")));
			lq.getParams().put("tenantID", sc.getPolicyContext().getTenantID(true).intValue());
			// fix the filtering 
			if (in.getWhereClause() != null) {
				ExpressionNode out = TableCatalogInformationSchemaTable.buildTenantTableNameTest(in.getWhereClause(), 
						tableNameColumn, tv, localName, scopeTableColumn);
				in.setWhereClause(out);
			}
		}

		
		List<ExpressionNode> decompAnd = ExpressionUtils.decomposeAndClause(in.getWhereClause());
		if (joinUserScope != null)
			decompAnd.add(joinUserScope);
		if (restrictTenant != null)
			decompAnd.add(restrictTenant);
		boolean showTenantColumn = 
					KnownVariables.SHOW_METADATA_EXTENSIONS.getValue(sc.getConnection().getVariableSource()).booleanValue();

		if (!showTenantColumn) {
			FunctionCall excludeTenantColumn = new FunctionCall(FunctionName.makeNotEquals(), new ColumnInstance(columnNameColumn,ours.toInstance()),
					new ActualLiteralExpression(AdaptiveMultitenantSchemaPolicyContext.TENANT_COLUMN,null));
			decompAnd.add(excludeTenantColumn);
		}
		in.setWhereClause(ExpressionUtils.safeBuildAnd(decompAnd));
		
		return lq;
	}
}
