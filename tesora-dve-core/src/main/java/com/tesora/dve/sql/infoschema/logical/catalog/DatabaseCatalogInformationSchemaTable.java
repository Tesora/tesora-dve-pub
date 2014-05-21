// OS_STATUS: public
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

import com.tesora.dve.common.catalog.MultitenantMode;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.infoschema.CatalogInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.LogicalInformationSchema;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.annos.InfoSchemaTable;
import com.tesora.dve.sql.infoschema.engine.LogicalQuery;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.structural.JoinSpecification;
import com.tesora.dve.sql.node.structural.JoinedTable;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.dml.AliasInformation;
import com.tesora.dve.sql.statement.dml.SelectStatement;

public class DatabaseCatalogInformationSchemaTable extends
		CatalogInformationSchemaTable {

	protected CatalogInformationSchemaColumn multitenantMode;
	protected CatalogInformationSchemaTable tenantTable;
	protected CatalogInformationSchemaColumn tenantTableDatabaseColumn;
	protected CatalogInformationSchemaColumn tenantTableNameColumn;
	protected CatalogInformationSchemaColumn databaseIDColumn;
	protected CatalogInformationSchemaColumn databaseNameColumn;
	
	public DatabaseCatalogInformationSchemaTable(Class<?> entKlass,
			InfoSchemaTable anno, String catTabName) {
		super(entKlass, anno, catTabName);
	}

	@Override
	protected void prepare(LogicalInformationSchema schema, DBNative dbn) {
		super.prepare(schema, dbn);
		multitenantMode = (CatalogInformationSchemaColumn) lookup("multitenant");
		tenantTable = (CatalogInformationSchemaTable) schema.lookup("tenant");
		tenantTableDatabaseColumn = (CatalogInformationSchemaColumn) tenantTable.lookup("database");
		tenantTableNameColumn = (CatalogInformationSchemaColumn) tenantTable.lookup("name");
		databaseIDColumn = (CatalogInformationSchemaColumn) lookup("id");
		databaseNameColumn = (CatalogInformationSchemaColumn) lookup("name");
	}
	
	@Override
	public boolean isLayered() {
		return true;
	}

	@Override
	public LogicalQuery explode(SchemaContext sc, LogicalQuery lq) {
		SelectStatement ss = lq.getQuery();
		List<ExpressionNode> decompAnd = ExpressionUtils.decomposeAndClause(ss.getWhereClause());
		MultitenantMode matches = null;
		FunctionCall matchingExpression = null;
		for(ExpressionNode en : decompAnd) {
			if (EngineConstant.FUNCTION.has(en)) {
				FunctionCall fc = (FunctionCall) en;
				if (EngineConstant.COLUMN.has(fc.getParametersEdge().get(0))) {
					ColumnInstance ci = (ColumnInstance) fc.getParametersEdge().get(0);
					if (ci.getColumn() == multitenantMode) {
						if (EngineConstant.CONSTANT.has(fc.getParametersEdge().get(1))) {
							LiteralExpression litex = (LiteralExpression) fc.getParametersEdge().get(1);
							if (litex.isStringLiteral()) {
								String value = litex.asString(sc);
								matches = MultitenantMode.toMode(value);
								matchingExpression = fc;
							}
						}
					}
				}
			}
		}
		if (matches == null)
			return lq;
		FunctionName op = matchingExpression.getFunctionName();
		if (!matches.isMT() && op.isEquals()) {
			// have to rewrite
		} else {
			return lq;
		}
	
		AliasInformation aliases = ss.getAliases();
		
		TableInstance tti = new TableInstance(tenantTable,null,
				aliases.buildNewAlias(new UnqualifiedName("tt")),(sc == null ? 0 : sc.getNextTable()),false);
		TableInstance dti = ss.getTablesEdge().get(0).getBaseTable();
		FunctionCall isNotOff =
			new FunctionCall(FunctionName.makeNotEquals(),
					new ColumnInstance(multitenantMode,dti),
					LiteralExpression.makeStringLiteral(MultitenantMode.OFF.getPersistentValue()));
		FunctionCall join =
			new FunctionCall(FunctionName.makeEquals(),
					new ColumnInstance(tenantTableDatabaseColumn,tti),
					new ColumnInstance(databaseIDColumn,dti));
		FunctionCall lhs = new FunctionCall(FunctionName.makeAnd(),isNotOff,join);
		lhs.setGrouped();
		JoinedTable jt = new JoinedTable(tti,lhs,JoinSpecification.LEFT_OUTER_JOIN);
		ss.getTablesEdge().get(0).addJoinedTable(jt);

		decompAnd.remove(matchingExpression);
		if (decompAnd.isEmpty())
			ss.setWhereClause(null);
		else
			ss.setWhereClause(ExpressionUtils.safeBuildAnd(decompAnd));
		
		ColumnInstance tenantName = new ColumnInstance(tenantTableNameColumn,tti);		
		LogicalInformationSchemaTable.BuildCoalesceColumnTraversal nca = new LogicalInformationSchemaTable.BuildCoalesceColumnTraversal(databaseNameColumn, tenantName);
		nca.traverse(ss);
		
		ss.getDerivedInfo().addLocalTable(tti.getTableKey());
				
		return lq;
	}
}
