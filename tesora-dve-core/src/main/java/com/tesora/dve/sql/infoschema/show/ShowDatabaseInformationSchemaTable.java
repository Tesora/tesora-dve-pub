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
import com.tesora.dve.sql.infoschema.InformationSchemaColumn;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.AbstractInformationSchema;
import com.tesora.dve.sql.infoschema.engine.ViewQuery;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.dml.SelectStatement;

public class ShowDatabaseInformationSchemaTable extends
		ShowInformationSchemaTable {

	protected InformationSchemaColumn multitenantMode;
	
	public ShowDatabaseInformationSchemaTable(
			LogicalInformationSchemaTable basedOn, UnqualifiedName viewName,
			UnqualifiedName pluralViewName, boolean isPriviledged,
			boolean isExtension) {
		super(basedOn, viewName, pluralViewName, isPriviledged, isExtension);
	}

	@Override
	protected void validate(AbstractInformationSchema ofView) {
		super.validate(ofView);
		multitenantMode = lookup(ShowSchema.Database.MULTITENANT);
	}
	
	@Override
	protected ViewQuery addAdditionalFiltering(ViewQuery vq) {
		// make sure that we only show multitenant databases
		SelectStatement ss = vq.getQuery();
		TableInstance ti = vq.getTable();
		ExpressionNode wc = ss.getWhereClause();
		List<ExpressionNode> decompAnd = ExpressionUtils.decomposeAndClause(wc);
		FunctionCall fc = new FunctionCall(FunctionName.makeEquals(),new ColumnInstance(multitenantMode,ti),LiteralExpression.makeStringLiteral(MultitenantMode.OFF.getPersistentValue()));
		decompAnd.add(fc);
		ss.setWhereClause(ExpressionUtils.safeBuildAnd(decompAnd));
		return vq;		
	}

	
}
