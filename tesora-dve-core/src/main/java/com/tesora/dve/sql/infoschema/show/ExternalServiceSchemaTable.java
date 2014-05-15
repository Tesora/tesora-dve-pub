// OS_STATUS: public
package com.tesora.dve.sql.infoschema.show;

import java.util.ArrayList;
import java.util.List;

import com.tesora.dve.externalservice.ExternalServiceFactory;
import com.tesora.dve.externalservice.ExternalServicePlugin;
import com.tesora.dve.resultset.ColumnSet;
import com.tesora.dve.resultset.IntermediateResultSet;
import com.tesora.dve.resultset.ResultRow;
import com.tesora.dve.sql.infoschema.InformationSchemaException;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class ExternalServiceSchemaTable extends ShowInformationSchemaTable {

	public ExternalServiceSchemaTable(LogicalInformationSchemaTable basedOn,
			UnqualifiedName viewName, UnqualifiedName pluralViewName,
			boolean isPriviledged, boolean isExtension) {
		super(basedOn, viewName, pluralViewName, isPriviledged, isExtension);
	}

	@Override
	public IntermediateResultSet executeUniqueSelect(SchemaContext sc,
			Name onName) {
		String serviceName = onName.getUnqualified().get();

		ColumnSet md = new ColumnSet();
		md.addColumn("Name", 255, "varchar", java.sql.Types.VARCHAR);
		md.addColumn("Status", 255, "varchar", java.sql.Types.VARCHAR);

		ResultRow rr = new ResultRow();
		boolean isRegistered = ExternalServiceFactory.isRegistered(serviceName);
		if (!isRegistered) {
			throw new InformationSchemaException("Cannot obtain status for external service '"
					+ serviceName + "' because the external service is not registered.");
		}

		try {
			ExternalServicePlugin plugin = ExternalServiceFactory.getInstance(serviceName);
			rr.addResultColumn(serviceName);
			rr.addResultColumn(plugin.status());
		} catch (Exception e) {
			throw new InformationSchemaException("Cannot obtain status for external service '"
					+ serviceName + "'", e);
		}

		List<ResultRow> rows = new ArrayList<ResultRow>();
		rows.add(rr);

		return new IntermediateResultSet(md, rows);
	}
}
