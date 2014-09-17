package com.tesora.dve.sql.infoschema.direct;

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

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.tesora.dve.sql.infoschema.InformationSchemaException;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.direct.DirectShowSchemaTable.TemporaryTableHandler;
import com.tesora.dve.sql.parser.InvokeParser;
import com.tesora.dve.sql.parser.ParserOptions;
import com.tesora.dve.sql.schema.PEViewTable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.ddl.PECreateViewStatement;

public class DirectTableGenerator extends DirectSchemaGenerator {

	private final InfoView view;
	private final String tableName;
	private final String pluralTableName;
	private final String viewName; 
	private final String viewDef;
	private final List<DirectColumnGenerator> columns;
	private TemporaryTableHandler tempTableHandler = null;
	
	public DirectTableGenerator(InfoView view, String tableName, String pluralTableName, String viewName, String viewDef,
			DirectColumnGenerator...colDefs) {
		this.view = view;
		this.tableName = tableName;
		this.pluralTableName = pluralTableName;
		this.viewDef = viewDef;
		this.viewName = viewName;
		this.columns = Arrays.asList(colDefs);
	}
	
	public DirectTableGenerator withExtension() {
		return super.withExtension();
	}
	
	public DirectTableGenerator withPrivilege() {
		return super.withPrivilege();
	}
	
	public DirectTableGenerator withTempHandler(TemporaryTableHandler tth) {
		this.tempTableHandler = tth;
		return this;
	}
	
	public DirectInformationSchemaTable generate(SchemaContext sc) {
		StringBuilder buf = new StringBuilder();
		buf.append("create view ").append(viewName).append(" as ").append(viewDef).append(" TABLE ( ");
		boolean first = true;
		for(DirectColumnGenerator dcg : columns) {
			if (first) first = false;
			else buf.append(", ");
			buf.append("`").append(dcg.getName()).append("` ").append(dcg.getType());
		}
		buf.append(" )");
		String vc = buf.toString();
		// I need a parser option here that defangs the view mode computation - so maybe just
		// an override
		ParserOptions opts = sc.getOptions();
		try {
			ParserOptions topts = ParserOptions.TEST.setResolve().setIgnoreMissingUser().setInfoSchemaView();
			List<Statement> stmts = InvokeParser.parse(vc, sc, Collections.EMPTY_LIST, topts);
			PECreateViewStatement pecs = (PECreateViewStatement) stmts.get(0);
			if (view == InfoView.SHOW) {
				return new DirectShowSchemaTable(sc,view,pecs.getViewTable(),
						new UnqualifiedName(tableName),
						(pluralTableName == null ? null : new UnqualifiedName(pluralTableName)),
						isPrivilege(),isExtension(), columns,
						tempTableHandler);
			} else {
				return new DirectInformationSchemaTable(sc,view,pecs.getViewTable(),
					new UnqualifiedName(tableName),
					(pluralTableName == null ? null : new UnqualifiedName(pluralTableName)),
					isPrivilege(),isExtension(), columns);
			}
		} finally {
			sc.setOptions(opts);
		}
	}
	
	/*
	// eventually we will be able to generate the user table stuff directly
	public void buildEntities(SchemaContext sc, CatalogSchema cs, CatalogDatabaseEntity db, int dmid, int storageid, 
			List<PersistedEntity> acc) throws PEException {
		if (view == InfoView.SHOW) return;
		String ct = String.format("create table %s ( %s )");
		List<Statement> stmts = InvokeParser.parse(ct,sc);
		PECreateTableStatement pecs = (PECreateTableStatement) stmts.get(0);
		DirectInformationSchemaTable.buildTableEntity(cs, db, dmid, storageid, acc, pecs.getTable());
	}
	*/
	
}
