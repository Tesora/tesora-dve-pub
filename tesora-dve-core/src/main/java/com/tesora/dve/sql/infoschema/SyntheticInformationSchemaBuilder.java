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
import com.tesora.dve.sql.infoschema.info.InfoSchemaEventsInformationSchemaTable;
import com.tesora.dve.sql.infoschema.info.InfoSchemaFilesInformationSchemaTable;
import com.tesora.dve.sql.infoschema.info.InfoSchemaGlobalTemporaryTablesInformationSchemaTable;
import com.tesora.dve.sql.infoschema.info.InfoSchemaPartitionsInformationSchemaTable;
import com.tesora.dve.sql.infoschema.info.InfoSchemaPluginsInformationSchemaTable;
import com.tesora.dve.sql.infoschema.info.InfoSchemaRoutinesInformationSchemaTable;
import com.tesora.dve.sql.infoschema.info.InfoSchemaSchemataSchemaTable;
import com.tesora.dve.sql.infoschema.info.InfoSchemaSessionTemporaryTablesInformationSchemaTable;
import com.tesora.dve.sql.infoschema.info.InfoSchemaVariablesInformationSchemaTable;
import com.tesora.dve.sql.infoschema.info.InfoSchemaViewInformationSchemaTable;
import com.tesora.dve.sql.infoschema.logical.EventsInformationSchemaTable;
import com.tesora.dve.sql.infoschema.logical.FilesInformationSchemaTable;
import com.tesora.dve.sql.infoschema.logical.FunctionStatusInformationSchemaTable;
import com.tesora.dve.sql.infoschema.logical.MysqlDBLogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.logical.PartitionsInformationSchemaTable;
import com.tesora.dve.sql.infoschema.logical.PluginsInformationSchemaTable;
import com.tesora.dve.sql.infoschema.logical.ProcedureStatusInformationSchemaTable;
import com.tesora.dve.sql.infoschema.logical.RoutinesInformationSchemaTable;
import com.tesora.dve.sql.infoschema.logical.StatusLogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.logical.TableStatusInformationSchemaTable;
import com.tesora.dve.sql.infoschema.logical.TriggerInformationSchemaTable;
import com.tesora.dve.sql.infoschema.logical.VariablesLogicalInformationSchemaTable;
import com.tesora.dve.sql.infoschema.logical.catalog.CatalogInformationSchemaTable;
import com.tesora.dve.sql.infoschema.logical.catalog.DatabaseCatalogInformationSchemaTable;
import com.tesora.dve.sql.infoschema.logical.catalog.ViewCatalogInformationSchemaTable;
import com.tesora.dve.sql.infoschema.mysql.MysqlDBInformationSchemaTable;
import com.tesora.dve.sql.infoschema.show.CreateDatabaseInformationSchemaTable;
import com.tesora.dve.sql.infoschema.show.CreateTableInformationSchemaTable;
import com.tesora.dve.sql.infoschema.show.ShowEventsInformationSchemaTable;
import com.tesora.dve.sql.infoschema.show.ShowFilesInformationSchemaTable;
import com.tesora.dve.sql.infoschema.show.ShowFunctionStatusInformationSchemaTable;
import com.tesora.dve.sql.infoschema.show.ShowInformationSchemaTable;
import com.tesora.dve.sql.infoschema.show.ShowMultitenantDatabaseSchemaTable;
import com.tesora.dve.sql.infoschema.show.ShowPartitionsInformationSchemaTable;
import com.tesora.dve.sql.infoschema.show.ShowProcedureStatusInformationSchemaTable;
import com.tesora.dve.sql.infoschema.show.ShowTableStatusInformationSchemaTable;
import com.tesora.dve.sql.infoschema.show.ShowTemplateOnDatabaseSchemaTable;
import com.tesora.dve.sql.infoschema.show.ShowTriggersInformationSchemaTable;
import com.tesora.dve.sql.infoschema.show.ShowView;
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
		final ShowInformationSchemaTable databaseInfoSchemaTable = (ShowInformationSchemaTable) showSchema.lookup("database");

		showSchema.addTable(null, new CreateTableInformationSchemaTable());
		showSchema.addTable(null, new ShowMultitenantDatabaseSchemaTable(databaseInfoSchemaTable));
		showSchema.addTable(null, new CreateDatabaseInformationSchemaTable());
		showSchema.addTable(null, new ShowTemplateOnDatabaseSchemaTable(databaseInfoSchemaTable));
		addMysqlDBTable(logicalSchema,mysqlSchema,dbn);
		addVariablesTable(logicalSchema, showSchema, infoSchema, dbn);
		addTableStatusTable(showSchema,dbn);
		addStatusTable(logicalSchema, showSchema, dbn);
		addTriggersTable(showSchema,dbn);
		addFilesTable(logicalSchema,showSchema,infoSchema,dbn);
		addPartitionsTable(logicalSchema,showSchema,infoSchema,dbn);
		addRoutinesTable(logicalSchema, infoSchema, dbn);
		addEventsTable(logicalSchema, infoSchema, showSchema, dbn);
		addProcedureStatusTable(showSchema, dbn);
		addFunctionStatusTable(showSchema, dbn);
		addSchemataTable(logicalSchema, infoSchema);
		addPluginsTable(logicalSchema, infoSchema, dbn);
		addViewsTable(logicalSchema, infoSchema, dbn);
		addTempTablesTables(logicalSchema, infoSchema, dbn);
	}
	
	/**
	 * @param logical
	 * @param infoSchema
	 * @param dbn
	 */
	private void addMysqlDBTable(LogicalInformationSchema logical, MysqlSchema infoSchema, DBNative dbn) {
		MysqlDBLogicalInformationSchemaTable baseTable = new MysqlDBLogicalInformationSchemaTable(logical);
		logical.addTable(null,baseTable);
		infoSchema.addTable(null,new MysqlDBInformationSchemaTable(baseTable));
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

	private void addTableStatusTable(ShowView showSchema, DBNative dbn) {
		// for show table status - the underlying logical table doesn't exist, so we can't add to the logical schema.
		TableStatusInformationSchemaTable tsist = new TableStatusInformationSchemaTable(dbn);
		showSchema.addTable(null, new ShowTableStatusInformationSchemaTable(tsist));
	}
	
	private void addTriggersTable(ShowView showSchema, DBNative dbn) {
		TriggerInformationSchemaTable trigs = new TriggerInformationSchemaTable(dbn);
		showSchema.addTable(null, new ShowTriggersInformationSchemaTable(trigs));
	}

	private void addFilesTable(LogicalInformationSchema logicalSchema,
			ShowView showSchema, InformationSchema infoSchema, DBNative dbn) {
		FilesInformationSchemaTable ist = new FilesInformationSchemaTable(dbn);
		logicalSchema.addTable(null,ist);
		showSchema.addTable(null,new ShowFilesInformationSchemaTable(ist));
		infoSchema.addTable(null,new InfoSchemaFilesInformationSchemaTable(ist));
	}

	private void addPartitionsTable(LogicalInformationSchema logicalSchema,
			ShowView showSchema, InformationSchema infoSchema, DBNative dbn) {
		PartitionsInformationSchemaTable ist = new PartitionsInformationSchemaTable(dbn);
		logicalSchema.addTable(null,ist);
		showSchema.addTable(null,new ShowPartitionsInformationSchemaTable(ist));
		infoSchema.addTable(null,new InfoSchemaPartitionsInformationSchemaTable(ist));
	}
	
	private void addRoutinesTable(LogicalInformationSchema logicalSchema,
			InformationSchema infoSchema, DBNative dbn) {
		RoutinesInformationSchemaTable ist = new RoutinesInformationSchemaTable(dbn);
		logicalSchema.addTable(null,ist);
		infoSchema.addTable(null,new InfoSchemaRoutinesInformationSchemaTable(ist));
	}

	private void addEventsTable(LogicalInformationSchema logicalSchema,
			InformationSchema infoSchema, ShowView showSchema, DBNative dbn) {
		EventsInformationSchemaTable ist = new EventsInformationSchemaTable(dbn);
		logicalSchema.addTable(null,ist);
		infoSchema.addTable(null,new InfoSchemaEventsInformationSchemaTable(ist));
		showSchema.addTable(null, new ShowEventsInformationSchemaTable(ist));
	}
	
	private void addProcedureStatusTable(ShowView showSchema, DBNative dbn) {
		ProcedureStatusInformationSchemaTable procedures = new ProcedureStatusInformationSchemaTable(dbn);
		showSchema.addTable(null, new ShowProcedureStatusInformationSchemaTable(procedures));
	}

	private void addFunctionStatusTable(ShowView showSchema, DBNative dbn) {
		FunctionStatusInformationSchemaTable functions = new FunctionStatusInformationSchemaTable(dbn);
		showSchema.addTable(null, new ShowFunctionStatusInformationSchemaTable(functions));
	}

	private void addSchemataTable(LogicalInformationSchema logicalSchema, InformationSchema infoSchema) {
		DatabaseCatalogInformationSchemaTable dbTab = (DatabaseCatalogInformationSchemaTable) logicalSchema.lookup("database");
		InfoSchemaSchemataSchemaTable schemataTab = new InfoSchemaSchemataSchemaTable(dbTab);
		infoSchema.addTable(null, schemataTab);
	}
	
	private void addPluginsTable(LogicalInformationSchema logicalSchema,
			InformationSchema infoSchema, DBNative dbn) {
		PluginsInformationSchemaTable ist = new PluginsInformationSchemaTable(dbn);
		logicalSchema.addTable(null,ist);
		infoSchema.addTable(null,new InfoSchemaPluginsInformationSchemaTable(ist));
	}

	private void addViewsTable(LogicalInformationSchema logicalSchema, InformationSchema infoSchema, DBNative dbn) {
		ViewCatalogInformationSchemaTable backing = (ViewCatalogInformationSchemaTable) logicalSchema.lookup("views");
		InfoSchemaViewInformationSchemaTable views = new InfoSchemaViewInformationSchemaTable(backing);
		infoSchema.addTable(null,views);
	}

	private void addTempTablesTables(LogicalInformationSchema logicalSchema, InformationSchema infoSchema, DBNative dbn) {
		CatalogInformationSchemaTable tempTables = (CatalogInformationSchemaTable) logicalSchema.lookup("temporary_table");
		InfoSchemaSessionTemporaryTablesInformationSchemaTable sessTabs =
				new InfoSchemaSessionTemporaryTablesInformationSchemaTable(tempTables);
		InfoSchemaGlobalTemporaryTablesInformationSchemaTable globTabs =
				new InfoSchemaGlobalTemporaryTablesInformationSchemaTable(tempTables);
		infoSchema.addTable(null,sessTabs);
		infoSchema.addTable(null, globTabs);
	}

	
}
