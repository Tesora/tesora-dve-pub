package com.tesora.dve.sql.infoschema.info;

import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.infoschema.LogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.engine.ViewQuery;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.dml.SelectStatement;

public class InfoSchemaSessionTemporaryTablesInformationSchemaTable extends
		InfoSchemaTemporaryTablesInformationSchemaTable {

	public InfoSchemaSessionTemporaryTablesInformationSchemaTable(
			LogicalInformationSchemaTable basedOn) {
		super(basedOn, new UnqualifiedName("temporary_tables"), false);
	}

	@Override
	public void annotate(SchemaContext sc, ViewQuery vq, SelectStatement in, TableKey onTK) {
		// filter on current session id only
	}
	
}
