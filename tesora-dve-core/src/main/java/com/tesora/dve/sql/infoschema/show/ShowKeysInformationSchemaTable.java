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
import java.util.Map;

import com.tesora.dve.common.catalog.ConstraintType;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.AbstractInformationSchema;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.computed.BackedComputedInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.computed.ComputedInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.computed.ConstantComputedInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.computed.SyntheticComputedInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.engine.NamedParameter;
import com.tesora.dve.sql.infoschema.engine.ViewQuery;
import com.tesora.dve.sql.infoschema.logical.catalog.KeyColumnCatalogInformationSchemaTable;
import com.tesora.dve.sql.node.expression.CastFunctionCall;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.structural.SortingSpecification;
import com.tesora.dve.sql.schema.ComplexPETable;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.QualifiedName;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.Pair;

public class ShowKeysInformationSchemaTable extends ShowInformationSchemaTable {
	
	private ComputedInformationSchemaColumn tableName;
	private ComputedInformationSchemaColumn keyName;
	private ComputedInformationSchemaColumn position;
	private ComputedInformationSchemaColumn databaseName;
	private ComputedInformationSchemaColumn constraint;
	
	public ShowKeysInformationSchemaTable(KeyColumnCatalogInformationSchemaTable backing) {
		super(backing, new UnqualifiedName("KEY"), new UnqualifiedName("KEYS"), false, false);

	}

	@Override
	public void prepare(AbstractInformationSchema view1, DBNative dbn) {
		tableName = new BackedComputedInformationSchemaColumn(InfoView.SHOW, backing.lookup("table_name"), new UnqualifiedName("Table")) {
			@Override
			public boolean isIdentColumn() { return true; }
			@Override
			public boolean isOrderByColumn() { return true; }
		};
		addColumn(null,tableName); 
		addColumn(null, new BackedComputedInformationSchemaColumn(InfoView.SHOW, backing.lookup("non_unique"), new UnqualifiedName("Non_unique")));
		keyName = new BackedComputedInformationSchemaColumn(InfoView.SHOW, backing.lookup("key_name"), new UnqualifiedName("Key_name"));
		addColumn(null, keyName);
		position = new BackedComputedInformationSchemaColumn(InfoView.SHOW, backing.lookup("position"), new UnqualifiedName("Seq_in_index"));
		addColumn(null, position);
		addColumn(null, new BackedComputedInformationSchemaColumn(InfoView.SHOW, backing.lookup("column_name"), new UnqualifiedName("Column_name")));
		addColumn(null, new BackedComputedInformationSchemaColumn(InfoView.SHOW, backing.lookup("collation"), new UnqualifiedName("Collation")));
		addColumn(null, new BackedComputedInformationSchemaColumn(InfoView.SHOW, backing.lookup("cardinality"), new UnqualifiedName("Cardinality")));
		addColumn(null, new BackedComputedInformationSchemaColumn(InfoView.SHOW, backing.lookup("length"), new UnqualifiedName("Sub_part")));
		addColumn(null, new SyntheticComputedInformationSchemaColumn(InfoView.SHOW, new UnqualifiedName("Packed"),
				LogicalInformationSchemaTable.buildStringType(dbn, 255)) {
			@Override
			public ExpressionNode buildReplacement(ColumnInstance subject) {
				return new CastFunctionCall(LiteralExpression.makeNullLiteral(), new UnqualifiedName("CHAR"));
			}

		});
		addColumn(null, new BackedComputedInformationSchemaColumn(InfoView.SHOW, backing.lookup("nullable"), new UnqualifiedName("Null")));
		addColumn(null, new BackedComputedInformationSchemaColumn(InfoView.SHOW, backing.lookup("index_type"), new UnqualifiedName("Index_type")));
		addColumn(null, new ConstantComputedInformationSchemaColumn(InfoView.SHOW, new UnqualifiedName("Comment"),
				LogicalInformationSchemaTable.buildStringType(dbn, 255),""));
		addColumn(null, new BackedComputedInformationSchemaColumn(InfoView.SHOW, backing.lookup("key_comment"), new UnqualifiedName("Index_comment")));
		// not registered - we use it for filtering but not to return 
		databaseName = new BackedComputedInformationSchemaColumn(InfoView.SHOW, backing.lookup("table_schema"), new UnqualifiedName("Database_name"));
		constraint = new BackedComputedInformationSchemaColumn(InfoView.SHOW, backing.lookup("constraint"), new UnqualifiedName("Constraint"));
	}
	
	// do our own sorting - we will sort first by table name, then key name, and finally seq_ind_index
	@Override
	public void addSorting(SelectStatement ss, TableInstance ti) {
		ComputedInformationSchemaColumn[] sorts = new ComputedInformationSchemaColumn[] { tableName, keyName, position };
		for(ComputedInformationSchemaColumn c : sorts) {
			SortingSpecification sort = new SortingSpecification(new ColumnInstance(null,c,ti), true);
			sort.setOrdering(Boolean.TRUE);
			ss.getOrderBysEdge().add(sort);
		}
	}
	
	@Override
	public Statement buildUniqueStatement(SchemaContext sc, Name objectName) {
		throw new SchemaException(Pass.SECOND, "Invalid show key command");
	}
	
	private Pair<String,String> inferScope(SchemaContext sc, List<Name> scoping) {
		if (scoping.size() > 2)
			throw new SchemaException(Pass.SECOND, "Overly qualified show keys statement");
		if (scoping.isEmpty())
			throw new SchemaException(Pass.SECOND, "Underly qualified show keys statement");
		return decomposeScope(sc,scoping);
	}
		
	@Override
	public IntermediateResultSet executeWhereSelect(SchemaContext sc, ExpressionNode wc, List<Name> scoping, ShowOptions options) {
		if (!sc.getTemporaryTableSchema().isEmpty()) {
			Pair<String,String> scope = inferScope(sc,scoping);
			QualifiedName qn = new QualifiedName(new UnqualifiedName(scope.getFirst()), new UnqualifiedName(scope.getSecond()));
			TableInstance matching = sc.getTemporaryTableSchema().buildInstance(sc, qn);
			if (matching != null)
				throw new SchemaException(Pass.PLANNER, "No support for show keys ... where for temporary tables");
		}
		return super.executeWhereSelect(sc, wc, scoping, options);
	}
	
	@Override
	public IntermediateResultSet executeLikeSelect(SchemaContext sc, String likeExpr, List<Name> scoping, ShowOptions options) {
		List<ResultRow> tempTab = null;
		if (!sc.getTemporaryTableSchema().isEmpty()) {
			Pair<String,String> scope = inferScope(sc,scoping);
			QualifiedName qn = new QualifiedName(new UnqualifiedName(scope.getFirst()), new UnqualifiedName(scope.getSecond()));
			TableInstance matching = sc.getTemporaryTableSchema().buildInstance(sc, qn);
			if (matching != null) {
				ComplexPETable ctab = (ComplexPETable) matching.getAbstractTable();
				tempTab = ctab.getShowKeys(sc);
			}
		}
		IntermediateResultSet irs = super.executeLikeSelect(sc, likeExpr, scoping, options);
		if (tempTab != null) {
			irs = new IntermediateResultSet(irs.getMetadata(),tempTab);
		}
		return irs;
	}
	
	@Override
	protected void handleScope(SchemaContext sc, SelectStatement ss, Map<String,Object> params, List<Name> scoping) {
		// for columns: table_name, db_name
		Pair<String,String> scope = inferScope(sc,scoping);
		TableInstance ti = ss.getBaseTables().get(0);
		ListSet<ExpressionNode> decompAnd = ExpressionUtils.decomposeAndClause(ss.getWhereClause());
		FunctionCall filterTable = new FunctionCall(FunctionName.makeEquals(),
				new ColumnInstance(null,tableName,ti),
				new NamedParameter(new UnqualifiedName("enctab")));
		filterTable.setGrouped();
		params.put("enctab", scope.getSecond());
		FunctionCall filterDB = new FunctionCall(FunctionName.makeEquals(),
				new ColumnInstance(null,databaseName, ti),
				new NamedParameter(new UnqualifiedName("encdb")));
		filterDB.setGrouped();
		params.put("encdb", scope.getFirst());
		decompAnd.add(filterTable);
		decompAnd.add(filterDB);
		ss.setWhereClause(ExpressionUtils.safeBuildAnd(decompAnd));
	}
	
	@Override
	protected ViewQuery addAdditionalFiltering(ViewQuery vq) {
		// make sure we filter out fks
		List<ExpressionNode> decompAnd = ExpressionUtils.decomposeAndClause(vq.getQuery().getWhereClause());
		TableInstance ti = vq.getTable();
		FunctionCall isNull = new FunctionCall(FunctionName.makeIs(), new ColumnInstance(null,constraint,ti),
				LiteralExpression.makeNullLiteral());
		isNull.setGrouped();
		FunctionCall notFK = new FunctionCall(FunctionName.makeNotEquals(),new ColumnInstance(null,constraint,ti),
				LiteralExpression.makeStringLiteral(ConstraintType.FOREIGN.getSQL()));
		notFK.setGrouped();
		FunctionCall orc = new FunctionCall(FunctionName.makeOr(),isNull,notFK);
		orc.setGrouped();
		decompAnd.add(orc);
		vq.getQuery().setWhereClause(ExpressionUtils.safeBuildAnd(decompAnd));
		return vq;
	}

}
