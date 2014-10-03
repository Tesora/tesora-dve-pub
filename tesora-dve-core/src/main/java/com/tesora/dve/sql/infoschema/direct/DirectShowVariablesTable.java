package com.tesora.dve.sql.infoschema.direct;

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

import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.infoschema.InfoView;
import com.tesora.dve.sql.infoschema.InformationSchemaColumn;
import com.tesora.dve.sql.infoschema.InformationSchemaException;
import com.tesora.dve.sql.infoschema.ShowOptions;
import com.tesora.dve.sql.infoschema.ShowSchemaBehavior;
import com.tesora.dve.sql.infoschema.engine.ViewQuery;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionAlias;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.schema.VariableScope;
import com.tesora.dve.sql.schema.VariableScopeKind;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.dml.AliasInformation;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.ColumnInstanceCollector;
import com.tesora.dve.sql.util.ListSet;

public class DirectShowVariablesTable extends DirectVariablesTable implements ShowSchemaBehavior {

	public static final String TABLE_NAME = "variables";
	public static final String SCOPE_COL_NAME = "Scope";
	public static final String NAME_COL_NAME = "Variable_name";
	public static final String VALUE_COL_NAME = "Value";
	
	public DirectShowVariablesTable(SchemaContext sc, 
			List<PEColumn> cols, 
			List<DirectColumnGenerator> columnGenerators) {
		super(sc, InfoView.SHOW, cols, new UnqualifiedName("variables"), null, columnGenerators);
	}

	public Statement buildShow(SchemaContext pc, VariableScope vs1,
			ExpressionNode likeExpr, ExpressionNode whereExpr) {
		// build the query, then use the abstract table to execute it
		List<ExpressionNode> whereClauses = new ArrayList<ExpressionNode>();
		TableInstance ti = buildOriginalFilter(whereClauses,pc,whereExpr,likeExpr);
		boolean showScope = false;
		if (vs1.getKind() == VariableScopeKind.SCOPED) {
			if (vs1.getScopeName() == null)
				showScope = true;
		}

		AliasInformation ai = new AliasInformation();
		ai.addAlias(ti.getAlias().getUnquotedName().get());

		List<InformationSchemaColumn> projCols = new ArrayList<InformationSchemaColumn>();
		if (showScope)
			projCols.add(lookup(SCOPE_COL_NAME));
		projCols.add(lookup(NAME_COL_NAME));
		projCols.add(lookup(VALUE_COL_NAME));
				
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
		ss.getDerivedInfo().addLocalTable(ti.getTableKey());

		ViewQuery vq = new ViewQuery(ss,null,ti);

		return execute(pc,this,vs1,vq,null);
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
						new ColumnInstance(lookup(NAME_COL_NAME),out),
						likeExpr));
			}
		}
		return out;
	}

	@Override
	public Statement buildShowPlural(SchemaContext sc, List<Name> scoping,
			ExpressionNode likeExpr, ExpressionNode whereExpr,
			ShowOptions options) {
		throw new InformationSchemaException("Illegal call to show variables.buildShowPlural");
	}

	@Override
	public Statement buildUniqueStatement(SchemaContext sc, Name objectName,
			ShowOptions opts) {
		throw new InformationSchemaException("Illegal call to show variables.buildUniqueStatement");
	}

	@Override
	public List<CatalogEntity> getLikeSelectEntities(SchemaContext sc,
			String likeExpr, List<Name> scoping, ShowOptions options,
			Boolean overrideRequiresPrivilegeValue) {
		throw new InformationSchemaException("Direct tables do not have catalog entities");
	}

}
