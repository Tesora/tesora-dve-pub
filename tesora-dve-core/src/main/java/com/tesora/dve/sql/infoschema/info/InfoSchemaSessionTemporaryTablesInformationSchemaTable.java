package com.tesora.dve.sql.infoschema.info;

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

import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.engine.ViewQuery;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.dml.SelectStatement;

public class InfoSchemaSessionTemporaryTablesInformationSchemaTable extends
		InfoSchemaTemporaryTablesInformationSchemaTable {

	public InfoSchemaSessionTemporaryTablesInformationSchemaTable(
			LogicalInformationSchemaTable basedOn) {
		super(basedOn, new UnqualifiedName("temporary_tables"), false);
	}

	@Override
	public void annotate(SchemaContext sc, ViewQuery vq, SelectStatement in, TableKey onTK) {
		// filter on current session id only
		TableInstance mine = null;
		for(TableKey tk : in.getDerivedInfo().getLocalTableKeys()) { 
			if (tk.getTable() == this) {
				mine = tk.toInstance();
			}
		}
		List<ExpressionNode> decompAnd = ExpressionUtils.decomposeAndClause(in.getWhereClause());
		decompAnd.add(new FunctionCall(FunctionName.makeEquals(),new ColumnInstance(sessionColumn,mine), LiteralExpression.makeLongLiteral(sc.getConnection().getConnectionId())));
		in.setWhereClause(ExpressionUtils.safeBuildAnd(decompAnd));
	}
	
}
