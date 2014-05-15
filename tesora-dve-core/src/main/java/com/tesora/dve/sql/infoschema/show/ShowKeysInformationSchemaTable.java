// OS_STATUS: public
package com.tesora.dve.sql.infoschema.show;

import java.util.List;
import java.util.Map;

import com.tesora.dve.common.catalog.ConstraintType;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.infoschema.ConstantSyntheticInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.InformationSchemaColumnView;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.SchemaView;
import com.tesora.dve.sql.infoschema.SyntheticInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.annos.InfoView;
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
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.util.ListSet;

public class ShowKeysInformationSchemaTable extends ShowInformationSchemaTable {
	
	private InformationSchemaColumnView tableName;
	private InformationSchemaColumnView keyName;
	private InformationSchemaColumnView position;
	private InformationSchemaColumnView databaseName;
	private InformationSchemaColumnView constraint;
	
	public ShowKeysInformationSchemaTable(KeyColumnCatalogInformationSchemaTable backing) {
		super(backing, new UnqualifiedName("KEY"), new UnqualifiedName("KEYS"), false, false);

	}

	@Override
	public void prepare(SchemaView view1, DBNative dbn) {
		tableName = new InformationSchemaColumnView(InfoView.SHOW, backing.lookup("table_name"), new UnqualifiedName("Table")) {
			@Override
			public boolean isIdentColumn() { return true; }
			@Override
			public boolean isOrderByColumn() { return true; }
		};
		addColumn(null,tableName); 
		addColumn(null, new InformationSchemaColumnView(InfoView.SHOW, backing.lookup("non_unique"), new UnqualifiedName("Non_unique")));
		keyName = new InformationSchemaColumnView(InfoView.SHOW, backing.lookup("key_name"), new UnqualifiedName("Key_name"));
		addColumn(null, keyName);
		position = new InformationSchemaColumnView(InfoView.SHOW, backing.lookup("position"), new UnqualifiedName("Seq_in_index"));
		addColumn(null, position);
		addColumn(null, new InformationSchemaColumnView(InfoView.SHOW, backing.lookup("column_name"), new UnqualifiedName("Column_name")));
		addColumn(null, new InformationSchemaColumnView(InfoView.SHOW, backing.lookup("collation"), new UnqualifiedName("Collation")));
		addColumn(null, new InformationSchemaColumnView(InfoView.SHOW, backing.lookup("cardinality"), new UnqualifiedName("Cardinality")));
		addColumn(null, new InformationSchemaColumnView(InfoView.SHOW, backing.lookup("length"), new UnqualifiedName("Sub_part")));
		addColumn(null, new SyntheticInformationSchemaColumn(InfoView.SHOW, new UnqualifiedName("Packed"),
				LogicalInformationSchemaTable.buildStringType(dbn, 255)) {
			@Override
			public ExpressionNode buildReplacement(ColumnInstance subject) {
				return new CastFunctionCall(LiteralExpression.makeNullLiteral(), new UnqualifiedName("CHAR"));
			}

		});
		addColumn(null, new InformationSchemaColumnView(InfoView.SHOW, backing.lookup("nullable"), new UnqualifiedName("Null")));
		addColumn(null, new InformationSchemaColumnView(InfoView.SHOW, backing.lookup("index_type"), new UnqualifiedName("Index_type")));
		addColumn(null, new ConstantSyntheticInformationSchemaColumn(InfoView.SHOW, new UnqualifiedName("Comment"),
				LogicalInformationSchemaTable.buildStringType(dbn, 255),""));
		addColumn(null, new InformationSchemaColumnView(InfoView.SHOW, backing.lookup("key_comment"), new UnqualifiedName("Index_comment")));
		// not registered - we use it for filtering but not to return 
		databaseName = new InformationSchemaColumnView(InfoView.SHOW, backing.lookup("table_schema"), new UnqualifiedName("Database_name"));
		constraint = new InformationSchemaColumnView(InfoView.SHOW, backing.lookup("constraint"), new UnqualifiedName("Constraint"));
	}
	
	// do our own sorting - we will sort first by table name, then key name, and finally seq_ind_index
	@Override
	public void addSorting(SelectStatement ss, TableInstance ti) {
		InformationSchemaColumnView[] sorts = new InformationSchemaColumnView[] { tableName, keyName, position };
		for(InformationSchemaColumnView c : sorts) {
			SortingSpecification sort = new SortingSpecification(new ColumnInstance(null,c,ti), true);
			sort.setOrdering(Boolean.TRUE);
			ss.getOrderBysEdge().add(sort);
		}
	}
	
	@Override
	public Statement buildUniqueStatement(SchemaContext sc, Name objectName) {
		throw new SchemaException(Pass.SECOND, "Invalid show key command");
	}
	
	@Override
	protected void handleScope(SchemaContext sc, SelectStatement ss, Map<String,Object> params, List<Name> scoping) {
		// for columns: table_name, db_name
		if (scoping.size() > 2)
			throw new SchemaException(Pass.SECOND, "Overly qualified show keys statement");
		if (scoping.isEmpty())
			throw new SchemaException(Pass.SECOND, "Underly qualified show keys statement");
		TableInstance ti = ss.getBaseTables().get(0);
		ListSet<ExpressionNode> decompAnd = ExpressionUtils.decomposeAndClause(ss.getWhereClause());
		Name table = scoping.get(0);
		FunctionCall filterTable = new FunctionCall(FunctionName.makeEquals(),
				new ColumnInstance(null,tableName,ti),
				new NamedParameter(new UnqualifiedName("enctab")));
		filterTable.setGrouped();
		params.put("enctab", table.getUnquotedName().get());
		FunctionCall filterDB = new FunctionCall(FunctionName.makeEquals(),
				new ColumnInstance(null,databaseName, ti),
				new NamedParameter(new UnqualifiedName("encdb")));
		filterDB.setGrouped();
		Name dbName = null;
		if (scoping.size() > 1)
			dbName = scoping.get(1);
		else
			dbName = sc.getCurrentDatabase().getName();
		params.put("encdb", dbName.getUnquotedName().get());
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
