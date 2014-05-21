// OS_STATUS: public
package com.tesora.dve.sql.infoschema.show;

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

import com.tesora.dve.common.ShowSchema;
import com.tesora.dve.common.catalog.MultitenantMode;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.infoschema.AbstractInformationSchemaColumnView;
import com.tesora.dve.sql.infoschema.InformationSchemaException;
import com.tesora.dve.sql.infoschema.SchemaView;
import com.tesora.dve.sql.infoschema.engine.ViewQuery;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.dml.SelectStatement;

// essentially a layered view
public class ShowMultitenantDatabaseSchemaTable extends
		ShowInformationSchemaTable {

	protected AbstractInformationSchemaColumnView modeColumn;
	
	public ShowMultitenantDatabaseSchemaTable(ShowInformationSchemaTable databaseView) {
		super(databaseView.getLogicalTable(), 
				new UnqualifiedName("multitenant database"), new UnqualifiedName("multitenant databases"), true, true);
		// add all the columns from databaseView
		for(AbstractInformationSchemaColumnView iscv: databaseView.getColumns(null))
			addColumn(null,iscv.copy());
	}

	@Override
	protected void validate(SchemaView ofView) {
		super.validate(ofView);
		modeColumn = lookup(ShowSchema.Database.MULTITENANT);
		if (modeColumn == null)
			throw new InformationSchemaException("Cannot find multitenant column in show table view");
	}

	@Override
	protected ViewQuery addAdditionalFiltering(ViewQuery vq) {
		// make sure that we only show multitenant databases
		SelectStatement ss = vq.getQuery();
		TableInstance ti = vq.getTable();
		ExpressionNode wc = ss.getWhereClause();
		List<ExpressionNode> decompAnd = ExpressionUtils.decomposeAndClause(wc);
		FunctionCall fc = new FunctionCall(FunctionName.makeNotEquals(),new ColumnInstance(modeColumn,ti),LiteralExpression.makeStringLiteral(MultitenantMode.OFF.getPersistentValue()));
		decompAnd.add(fc);
		ss.setWhereClause(ExpressionUtils.safeBuildAnd(decompAnd));
		return vq;
	}
	
}
