package com.tesora.dve.sql.infoschema;

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
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.infoschema.info.InfoSchemaVariablesInformationSchemaTable;
import com.tesora.dve.sql.infoschema.logical.StatusLogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.logical.VariablesLogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.show.StatusInformationSchemaTable;
import com.tesora.dve.sql.infoschema.show.ShowVariablesInformationSchemaTable;
import com.tesora.dve.sql.schema.UnqualifiedName;

// synthetic info schema tables come in two varieties:
// - they may transform other info schema tables 
// - they may be pass through - we send the show down to the p sites and transform the results.
public class SyntheticInformationSchemaBuilder implements InformationSchemaBuilder {

	@Override
	public void populate(LogicalInformationSchema logicalSchema,
			InformationSchema infoSchema, ShowView showSchema,
			MysqlSchema mysqlSchema, DBNative dbn) throws PEException {
		addVariablesTable(logicalSchema, showSchema, infoSchema, dbn);
		addStatusTable(logicalSchema, showSchema, dbn);
	}
	
	private void addVariablesTable(LogicalInformationSchema logical, ShowView showSchema, InformationSchema infoSchema, DBNative dbn) {
		VariablesLogicalInformationSchemaTable baseTable = new VariablesLogicalInformationSchemaTable(dbn);
		logical.addTable(null,baseTable);
		showSchema.addTable(null,new ShowVariablesInformationSchemaTable(baseTable));
		infoSchema.addTable(null, new InfoSchemaVariablesInformationSchemaTable(baseTable, 
				new UnqualifiedName(VariablesLogicalInformationSchemaTable.GLOBAL_TABLE_NAME), 
				VariablesLogicalInformationSchemaTable.GLOBAL_TABLE_NAME));
		infoSchema.addTable(null, new InfoSchemaVariablesInformationSchemaTable(baseTable, 
				new UnqualifiedName(VariablesLogicalInformationSchemaTable.SESSION_TABLE_NAME),
				VariablesLogicalInformationSchemaTable.SESSION_TABLE_NAME));
	}

	private void addStatusTable(LogicalInformationSchema logical, ShowView showSchema, DBNative dbn) {
		StatusLogicalInformationSchemaTable baseTable = new StatusLogicalInformationSchemaTable(dbn);
		logical.addTable(null,baseTable);
		showSchema.addTable(null,new StatusInformationSchemaTable(baseTable));
	}

	
}
