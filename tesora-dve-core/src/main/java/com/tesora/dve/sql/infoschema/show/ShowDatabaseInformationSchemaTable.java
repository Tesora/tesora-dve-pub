// OS_STATUS: public
package com.tesora.dve.sql.infoschema.show;

import java.util.List;

import com.tesora.dve.common.ShowSchema;
import com.tesora.dve.common.catalog.MultitenantMode;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.infoschema.AbstractInformationSchemaColumnView;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
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

public class ShowDatabaseInformationSchemaTable extends
		ShowInformationSchemaTable {

	protected AbstractInformationSchemaColumnView multitenantMode;
	
	public ShowDatabaseInformationSchemaTable(
			LogicalInformationSchemaTable basedOn, UnqualifiedName viewName,
			UnqualifiedName pluralViewName, boolean isPriviledged,
			boolean isExtension) {
		super(basedOn, viewName, pluralViewName, isPriviledged, isExtension);
	}

	@Override
	protected void validate(SchemaView ofView) {
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
