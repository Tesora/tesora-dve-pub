package com.tesora.dve.sql.infoschema.info;

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

import com.tesora.dve.db.DBNative;
import com.tesora.dve.sql.infoschema.InformationSchemaColumnView;
import com.tesora.dve.sql.infoschema.InformationSchemaTableView;
import com.tesora.dve.sql.infoschema.SchemaView;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.logical.DistributionLogicalTable;
import com.tesora.dve.sql.schema.UnqualifiedName;

public class InfoSchemaDistributionsInformationSchemaTable extends InformationSchemaTableView {

	public InfoSchemaDistributionsInformationSchemaTable(DistributionLogicalTable dlt) {
		super(InfoView.INFORMATION, dlt, new UnqualifiedName("distributions"),null,false,true);
	}

	@Override
	public void prepare(SchemaView schemaView, DBNative dbn) {
		DistributionLogicalTable dlt = (DistributionLogicalTable) getLogicalTable();
		addColumn(null,new InformationSchemaColumnView(InfoView.INFORMATION, dlt.lookup("database_name"), 
				new UnqualifiedName("database_name")));
		addColumn(null,new InformationSchemaColumnView(InfoView.INFORMATION, dlt.lookup("table_name"),
				new UnqualifiedName("table_name")));
		addColumn(null,new InformationSchemaColumnView(InfoView.INFORMATION, dlt.lookup("column_name"),
				new UnqualifiedName("column_name")));
		addColumn(null,new InformationSchemaColumnView(InfoView.INFORMATION, dlt.lookup("dvposition"),
				new UnqualifiedName("vector_position")));
		addColumn(null,new InformationSchemaColumnView(InfoView.INFORMATION, dlt.lookup("model_name"),
				new UnqualifiedName("model_type")));
		addColumn(null,new InformationSchemaColumnView(InfoView.INFORMATION, dlt.lookup("range_name"),
				new UnqualifiedName("model_name")));		
	}

}
