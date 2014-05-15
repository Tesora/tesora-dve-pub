// OS_STATUS: public
package com.tesora.dve.sql.infoschema.info;

import java.util.List;

import com.tesora.dve.db.DBNative;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.infoschema.ConstantSyntheticInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.InformationSchemaColumnView;
import com.tesora.dve.sql.infoschema.InformationSchemaTableView;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.SchemaView;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.engine.ViewQuery;
import com.tesora.dve.sql.infoschema.logical.catalog.KeyColumnCatalogInformationSchemaTable;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.dml.SelectStatement;

public class InfoSchemaKeyColumnUsageInformationSchemaTable extends
		InformationSchemaTableView {

	private InformationSchemaColumnView synthetic;
	
	public InfoSchemaKeyColumnUsageInformationSchemaTable(KeyColumnCatalogInformationSchemaTable keyTable) {
		super(InfoView.INFORMATION, keyTable, new UnqualifiedName("key_column_usage"), null, false, false);
	}

	@Override
	public void prepare(SchemaView schemaView, DBNative dbn) {
		KeyColumnCatalogInformationSchemaTable keyTable = (KeyColumnCatalogInformationSchemaTable) getLogicalTable();
		addColumn(null,new ConstantSyntheticInformationSchemaColumn(InfoView.INFORMATION, new UnqualifiedName("constraint_catalog"),
				LogicalInformationSchemaTable.buildStringType(dbn, 255), "def"));
		addColumn(null,new ConstantSyntheticInformationSchemaColumn(InfoView.INFORMATION, new UnqualifiedName("table_catalog"),
				LogicalInformationSchemaTable.buildStringType(dbn, 255), "def"));
		addColumn(null,new InformationSchemaColumnView(InfoView.INFORMATION, keyTable.lookup("table_schema"), 
				new UnqualifiedName("table_schema")));
		addColumn(null,new InformationSchemaColumnView(InfoView.INFORMATION, keyTable.lookup("table_name"), 
				new UnqualifiedName("table_name")));
		addColumn(null,new InformationSchemaColumnView(InfoView.INFORMATION, keyTable.lookup("column_name"), 
				new UnqualifiedName("column_name")));
		addColumn(null,new InformationSchemaColumnView(InfoView.INFORMATION, keyTable.lookup("position"), 
				new UnqualifiedName("ordinal_position")));
		addColumn(null,new InformationSchemaColumnView(InfoView.INFORMATION, keyTable.lookup("referenced_schema_name"), 
				new UnqualifiedName("referenced_table_schema")));
		addColumn(null,new InformationSchemaColumnView(InfoView.INFORMATION, keyTable.lookup("referenced_table_name"), 
				new UnqualifiedName("referenced_table_name")));
		addColumn(null,new InformationSchemaColumnView(InfoView.INFORMATION, keyTable.lookup("referenced_column_name"), 
				new UnqualifiedName("referenced_column_name")));
		synthetic = new InformationSchemaColumnView(InfoView.INFORMATION, keyTable.lookup("synthetic"), new UnqualifiedName("synthetic"));
		super.prepare(schemaView,dbn);
	}
	
	@Override
	public void validate(SchemaView ofView) {
	}
	
	@Override
	public void annotate(SchemaContext sc, ViewQuery vq, SelectStatement in, TableKey onTK) {
		ColumnInstance ref = new ColumnInstance(synthetic,onTK.toInstance());
		FunctionCall nonSynthOnly = new FunctionCall(FunctionName.makeEquals(), ref, 
				LiteralExpression.makeLongLiteral(0));
		nonSynthOnly.setGrouped();
		List<ExpressionNode> decompAnd = ExpressionUtils.decomposeAndClause(in.getWhereClause());
		decompAnd.add(nonSynthOnly);
		in.setWhereClause(ExpressionUtils.safeBuildAnd(decompAnd));
	}
}
