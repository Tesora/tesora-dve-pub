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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import com.tesora.dve.common.DBHelper;
import com.tesora.dve.common.PEConstants;
import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.FKMode;
import com.tesora.dve.common.catalog.MultitenantMode;
import com.tesora.dve.common.catalog.TemplateMode;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.errmap.ErrorMapper;
import com.tesora.dve.errmap.FormattedErrorInfo;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PEMappedRuntimeException;
import com.tesora.dve.persist.PersistedEntity;
import com.tesora.dve.sql.infoschema.direct.DirectSchemaBuilder;
import com.tesora.dve.sql.infoschema.persist.CatalogSchema;
import com.tesora.dve.sql.parser.InvokeParser;
import com.tesora.dve.sql.parser.ParserOptions;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.PEPersistentGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.UnqualifiedName;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.ddl.DDLStatement;
import com.tesora.dve.sql.statement.ddl.PECreateTableStatement;
import com.tesora.dve.sql.transexec.TransientExecutionEngine;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.upgrade.CatalogSchemaGenerator;

public final class InformationSchemas {

	protected final InformationSchema infoSchema;
	protected final ShowView show;
	protected final MysqlSchema mysql;
	protected final PEDatabase catalog;
	
	private InformationSchemas(InformationSchema isv, ShowView sv, MysqlSchema msv, PEDatabase pdb) {
		this.infoSchema = isv;
		this.show = sv;
		this.mysql = msv;
		this.catalog = pdb;
	}
	
	public InformationSchema getInfoSchema() {
		return infoSchema;
	}
	
	public ShowView getShowSchema() {
		return this.show;
	}
	
	public ShowSchemaBehavior lookupShowTable(UnqualifiedName unq) {
		return getShowSchema().lookupTable(unq);
	}
	
	public MysqlSchema getMysqlSchema() {
		return this.mysql;
	}
	
	public List<PersistedEntity> buildEntities(int groupid, int modelid, String charSet, String collation) throws PEException {
		CatalogSchema cs = new CatalogSchema();
		ArrayList<PersistedEntity> acc = new ArrayList<PersistedEntity>();
		infoSchema.buildEntities(cs,groupid, modelid, charSet, collation, acc);
		show.buildEntities(cs, groupid, modelid, charSet, collation, acc);
		mysql.buildEntities(cs, groupid, modelid, charSet, collation, acc);
		return acc;
	}
	
	public static InformationSchemas build(DBNative dbn, CatalogDAO c, Properties props) throws PEException {
		try {
			InformationSchema informationSchema = new InformationSchema();
			ShowView showSchema = new ShowView();
			MysqlSchema mysqlSchema = new MysqlSchema();
			PEDatabase catSchema = buildCatalogSchema(c,dbn,props);
			
			// make the builders for each schema & then build them.
			InformationSchemaBuilder builders[] = new InformationSchemaBuilder[] {
					// the order these are built in is important
					new DirectSchemaBuilder(catSchema)
			};
			for(InformationSchemaBuilder isb : builders)
				isb.populate(informationSchema, showSchema, mysqlSchema, dbn);
			informationSchema.freeze(dbn);
			showSchema.freeze(dbn);
			mysqlSchema.freeze(dbn);
			return new InformationSchemas(informationSchema,showSchema,mysqlSchema,
					catSchema);
		} catch (PEException pe) {
			throw pe;
		} catch (PEMappedRuntimeException se) {
			FormattedErrorInfo fei = ErrorMapper.makeResponse(se);
			throw new PEException("Unable to initialize information schema: " +
					(fei == null ? se.getMessage() : fei.getErrorMessage()), se);
		} catch (Throwable t) {
			throw new PEException("Unable to initialize information schema",t);
		}
	}
	
	public InformationSchemaDatabase buildPEDatabase(SchemaContext sc, UserDatabase udb) {
		if (InfoView.INFORMATION.getUserDatabaseName().equals(udb.getName()))
			return new InformationSchemaDatabase(sc, udb, infoSchema);
		else if (InfoView.MYSQL.getUserDatabaseName().equals(udb.getName()))
			return new InformationSchemaDatabase(sc, udb, mysql);
		return null;
	}
	
	private static PEDatabase buildCatalogSchema(CatalogDAO c, DBNative dbn, Properties catalogProps) throws Throwable {
		if (c == null) return null; // for now
		String database = catalogProps.getProperty(DBHelper.CONN_DBNAME, PEConstants.CATALOG);
		TransientExecutionEngine tee = new TransientExecutionEngine(database,dbn.getTypeCatalog());
		ParserOptions opts = ParserOptions.TEST.setResolve().setIgnoreMissingUser();
		SchemaContext sc = tee.getPersistenceContext();
		
		PEPersistentGroup catalogGroup = new PEPersistentGroup(sc,new UnqualifiedName(PEConstants.SYSTEM_GROUP_NAME), Collections.EMPTY_LIST);
		PEDatabase pdb = new PEDatabase(sc,new UnqualifiedName(database),catalogGroup,new Pair<Name,TemplateMode>(null, TemplateMode.OPTIONAL), MultitenantMode.OFF, FKMode.STRICT, "","");
		pdb.setID(-1);
		
		tee.setCurrentDatabase(pdb);
		
		String[] decls = CatalogSchemaGenerator.buildCreateCurrentSchema(c,catalogProps);
		for(String s : decls) {
			if (s.startsWith("create")) {
				sc.refresh(true);
				List<Statement> stmts = InvokeParser.parse(InvokeParser.buildInputState(s,sc), opts, sc).getStatements();
				for(Statement stmt : stmts) {
					DDLStatement ddl = (DDLStatement) stmt;
					if (ddl.getAction() == CatalogModificationExecutionStep.Action.CREATE) {
						PECreateTableStatement pects = (PECreateTableStatement) stmt;
						pdb.getSchema().addTable(sc, pects.getTable());						
					}
				}
			}
		}
				
		return pdb;
	}
}
