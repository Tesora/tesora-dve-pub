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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.infoschema.InformationSchemaColumn;
import com.tesora.dve.sql.infoschema.InformationSchemaException;
import com.tesora.dve.sql.infoschema.AbstractInformationSchema;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.computed.BackedComputedInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.engine.ViewQuery;
import com.tesora.dve.sql.infoschema.logical.VariablesLogicalInformationSchemaTable;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.VariableScope;
import com.tesora.dve.sql.schema.VariableScopeKind;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.dml.AliasInformation;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.ColumnInstanceCollector;
import com.tesora.dve.sql.util.ListSet;

// show variables is still not really supported.  we return a fake result set.
public class ShowVariablesInformationSchemaTable extends ShowInformationSchemaTable {
	
	protected VariablesLogicalInformationSchemaTable logicalTable = null;
	
	public ShowVariablesInformationSchemaTable(VariablesLogicalInformationSchemaTable logicalTable) {
		super(logicalTable, logicalTable.getName().getUnqualified(), logicalTable.getName().getUnqualified(), false, false);
		
		this.logicalTable = logicalTable;
		addColumn(null,new BackedComputedInformationSchemaColumn(InfoView.SHOW, logicalTable.lookup(VariablesLogicalInformationSchemaTable.SCOPE_COL_NAME), new UnqualifiedName(VariablesLogicalInformationSchemaTable.SCOPE_COL_NAME)));
		addColumn(null,new BackedComputedInformationSchemaColumn(InfoView.SHOW, logicalTable.lookup(VariablesLogicalInformationSchemaTable.NAME_COL_NAME), new UnqualifiedName(VariablesLogicalInformationSchemaTable.NAME_COL_NAME)));
		addColumn(null,new BackedComputedInformationSchemaColumn(InfoView.SHOW, logicalTable.lookup(VariablesLogicalInformationSchemaTable.VALUE_COL_NAME), new UnqualifiedName(VariablesLogicalInformationSchemaTable.VALUE_COL_NAME)));
	}

    public ViewQuery buildUniqueSelect(SchemaContext sc, Name onName) {
    	throw new InformationSchemaException("No support for show variable");
    }
	
	public Statement buildShow(SchemaContext pc, VariableScope vs1,
			ExpressionNode likeExpr, ExpressionNode whereExpr) {
		// first we're going to build a query and slap the appropriate scope on it; then we'll have the logical table execute it.
		List<ExpressionNode> whereClauses = new ArrayList<ExpressionNode>();
		TableInstance ti = buildOriginalFilter(whereClauses,pc,whereExpr,likeExpr);
		String filterBy = null;
		boolean showScope = false;
		if (vs1.getKind() == VariableScopeKind.GLOBAL) {
			filterBy = VariablesLogicalInformationSchemaTable.GLOBAL_TABLE_NAME;
		} else if (vs1.getKind() == VariableScopeKind.SCOPED) {
			filterBy = vs1.getScopeName();
			if (filterBy == null) { // not set
				filterBy = VariablesLogicalInformationSchemaTable.SCOPED_TABLE_NAME;
				showScope = true;
			}
		} else {
			filterBy = VariablesLogicalInformationSchemaTable.SESSION_TABLE_NAME;
		}
		whereClauses.add(new FunctionCall(FunctionName.makeEquals(),
				new ColumnInstance(lookup(VariablesLogicalInformationSchemaTable.SCOPE_COL_NAME),ti),
				LiteralExpression.makeStringLiteral(filterBy)));

		AliasInformation ai = new AliasInformation();
		ai.addAlias(ti.getAlias().getUnquotedName().get());

		List<InformationSchemaColumn> projCols = new ArrayList<InformationSchemaColumn>();
		if (showScope)
			projCols.add(lookup(VariablesLogicalInformationSchemaTable.SCOPE_COL_NAME));
		projCols.add(lookup(VariablesLogicalInformationSchemaTable.NAME_COL_NAME));
		projCols.add(lookup(VariablesLogicalInformationSchemaTable.VALUE_COL_NAME));
				
		List<ExpressionNode> projection = new ArrayList<ExpressionNode>();

		for(InformationSchemaColumn c : projCols) {
			ColumnInstance ci = new ColumnInstance(c,ti);
			ExpressionAlias ea = new ExpressionAlias(ci, ci.buildAlias(pc), true);
			ai.addAlias(ea.getAlias().get());
			projection.add(ea);
		}

		SelectStatement ss = new SelectStatement(ai)
			.setTables(ti)
			.setProjection(projection)
			.setWhereClause(ExpressionUtils.safeBuildAnd(whereClauses));

		ViewQuery vq = new ViewQuery(ss,Collections.EMPTY_MAP,ti);

		return logicalTable.execute(pc, vq, null);
	}

	private TableInstance buildOriginalFilter(List<ExpressionNode> acc, SchemaContext pc, ExpressionNode whereExpr, ExpressionNode likeExpr) {
		TableInstance out = null;
		if (whereExpr != null) {
			ListSet<ColumnInstance> cols = ColumnInstanceCollector.getColumnInstances(whereExpr);
			out = cols.get(0).getTableInstance();
			acc.add(whereExpr);
		} else {
			// build one - there won't be a table instance yet, so create one
			out = new TableInstance(this,getName(),
					new UnqualifiedName("a"),
					pc.getNextTable(),false);
			if (likeExpr != null) {
				acc.add(new FunctionCall(FunctionName.makeLike(),
						new ColumnInstance(lookup(VariablesLogicalInformationSchemaTable.NAME_COL_NAME),out),
						likeExpr));
			}
		}
		return out;
	}
	
	
	@Override
	protected void validate(AbstractInformationSchema ofView) {
	}

	
	
}
