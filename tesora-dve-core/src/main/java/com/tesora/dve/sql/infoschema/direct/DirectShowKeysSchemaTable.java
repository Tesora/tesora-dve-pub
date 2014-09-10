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

import java.util.Collections;
import java.util.List;

import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.infoschema.InformationSchemaColumn;
import com.tesora.dve.sql.infoschema.InformationSchemaException;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.engine.LogicalCatalogQuery;
import com.tesora.dve.sql.infoschema.show.ShowOptions;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.schema.ComplexPETable;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEViewTable;
import com.tesora.dve.sql.schema.QualifiedName;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.ddl.SchemaQueryStatement;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.sql.util.UnaryFunction;

public class DirectShowKeysSchemaTable extends DirectShowSchemaTable {

	public DirectShowKeysSchemaTable(SchemaContext sc, InfoView view,
			PEViewTable viewTab, UnqualifiedName viewName,
			UnqualifiedName pluralViewName, boolean privileged,
			boolean extension, List<DirectColumnGenerator> columnGenerators) {
		super(sc, view, viewTab, viewName, pluralViewName, privileged,
				extension, columnGenerators);
	}

	
	@Override
	public Statement buildShowPlural(SchemaContext sc, List<Name> scoping,
			ExpressionNode likeExpr, ExpressionNode whereExpr,
			ShowOptions options) {
		if (!sc.getTemporaryTableSchema().isEmpty()) {
			Pair<String,String> scope = inferScope(sc,scoping);
			QualifiedName qn = new QualifiedName(new UnqualifiedName(scope.getFirst()), new UnqualifiedName(scope.getSecond()));
			TableInstance matching = sc.getTemporaryTableSchema().buildInstance(sc, qn);
			if (matching != null) {
				ComplexPETable ctab = (ComplexPETable) matching.getAbstractTable();
				List<ResultRow> tempTab = ctab.getShowKeys(sc);
				List<List<InformationSchemaColumn>> proj = 
						Functional.apply(getColumns(sc), new UnaryFunction<List<InformationSchemaColumn>,InformationSchemaColumn>() {

							@Override
							public List<InformationSchemaColumn> evaluate(
									InformationSchemaColumn object) {
								return Collections.singletonList(object);
							}
							
						});
				ColumnSet cs = LogicalCatalogQuery.buildProjectionMetadata(sc,proj,null,null);
				IntermediateResultSet irs = new IntermediateResultSet(cs,tempTab);
				return new SchemaQueryStatement(false,getName().get(),irs);
			}
		}
		return super.buildShowPlural(sc, scoping, likeExpr, whereExpr, options);
	}
	
	private Pair<String,String> inferScope(SchemaContext sc, List<Name> scoping) {
		if (scoping.size() > 2)
			throw new InformationSchemaException("Overly qualified show keys statement");
		if (scoping.isEmpty())
			throw new InformationSchemaException("Underly qualified show keys statement");
		return decomposeScope(sc,scoping);
	}
		
	protected Pair<String,String> decomposeScope(SchemaContext sc, List<Name> scoping) {
		Name tablen = scoping.get(0);
		Name dbn = (scoping.size() > 1 ? scoping.get(1) : null);
		if (dbn == null && sc != null) dbn = sc.getCurrentDatabase().getName();
		String tableName = (tablen == null ? null : tablen.get());
		String dbName = (dbn == null ? null : dbn.get());
		return new Pair<String,String>(dbName,tableName);		
	}

	
}
