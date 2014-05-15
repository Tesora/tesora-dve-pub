// OS_STATUS: public
package com.tesora.dve.sql.infoschema.show;

import java.util.List;

import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.sql.infoschema.InformationSchemaColumnView;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaColumn;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;

/**
 * Boiler-plate code for InformationSchema tables that are currently short-circuited
 * to just return an empty result set.  Once implemented fully, this class should
 * be removed from the relevant table class hierarchy
 */
public abstract class ShowEmptyInformationSchemaTable extends ShowInformationSchemaTable {

	public ShowEmptyInformationSchemaTable(
			LogicalInformationSchemaTable basedOn, UnqualifiedName viewName,
			UnqualifiedName pluralViewName, boolean isPriviledged, boolean isExtension) {
		super(basedOn, viewName, pluralViewName, isPriviledged, isExtension);
		LogicalInformationSchemaColumn identOrderByColumn = basedOn.getIdentOrderByColumn();
		for (LogicalInformationSchemaColumn lisc : basedOn.getColumns(null)) {
			if (lisc == identOrderByColumn) {
				identColumn = new InformationSchemaColumnView(InfoView.SHOW, lisc, lisc.getName().getUnqualified()) {
					@Override
					public boolean isIdentColumn() {
						return true;
					}

					@Override
					public boolean isOrderByColumn() {
						return true;
					}
				};
				addColumn(null, identColumn);
			} else {
				addColumn(null, new InformationSchemaColumnView(InfoView.SHOW, lisc, lisc.getName().getUnqualified()));
			}
		}
	}

	@Override
	public IntermediateResultSet executeLikeSelect(SchemaContext sc,
			String likeExpr, List<Name> scoping, ShowOptions options) {
		return buildEmptyResultSet(sc);
	}

	@Override
	public IntermediateResultSet executeUniqueSelect(SchemaContext sc,
			Name onName) {
		return buildEmptyResultSet(sc);
	}

	@Override
	public IntermediateResultSet executeWhereSelect(SchemaContext sc,
			ExpressionNode wc, List<Name> scoping, ShowOptions options) {
		return buildEmptyResultSet(sc);
	}
}