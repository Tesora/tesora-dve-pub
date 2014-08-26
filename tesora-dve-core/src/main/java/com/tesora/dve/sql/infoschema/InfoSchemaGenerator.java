package com.tesora.dve.sql.infoschema;

import java.util.Collections;
import java.util.List;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.persist.PersistedEntity;
import com.tesora.dve.sql.ParserException;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.persist.CatalogDatabaseEntity;
import com.tesora.dve.sql.infoschema.persist.CatalogSchema;
import com.tesora.dve.sql.parser.InvokeParser;
import com.tesora.dve.sql.parser.ParserOptions;
import com.tesora.dve.sql.schema.PEViewTable;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.Statement;
import com.tesora.dve.sql.statement.ddl.PECreateTableStatement;
import com.tesora.dve.sql.statement.ddl.PECreateViewStatement;
import com.tesora.dve.sql.transexec.TransientExecutionEngine;

public class InfoSchemaGenerator {

	private final String columnDef;
	private final String viewDef;
	private final InfoView view;
	private final String viewName;
	private final boolean privileged;
	private final boolean extension;
	private final String identColumn;
	private final String orderByColumn;
	
	public InfoSchemaGenerator(InfoView view, String name, String columnDef, String viewDef,
			boolean privileged, boolean extension, String identColumn, String orderByColumn) {
		this.columnDef = columnDef;
		this.viewDef = viewDef;
		this.view = view;
		this.viewName = name;
		this.privileged = privileged;
		this.extension = extension;
		this.identColumn = identColumn;
		this.orderByColumn = orderByColumn;
	}

	public ViewBasedInformationSchemaTableView generate(SchemaContext sc) {
		String vc = String.format("create view %s as %s TABLE ( %s )",
				viewName,viewDef,columnDef);
		// I need a parser option here that defangs the view mode computation - so maybe just
		// an override
		ParserOptions opts = sc.getOptions();
		try {
			ParserOptions topts = ParserOptions.TEST.setResolve().setIgnoreMissingUser().setInfoSchemaView();
			List<Statement> stmts = InvokeParser.parse(vc, sc, Collections.EMPTY_LIST, topts);
			PECreateViewStatement pecs = (PECreateViewStatement) stmts.get(0);
			// probably eventually we will lookup the impl class by reflection
			return new ViewBasedInformationSchemaTableView(sc,view,pecs.getViewTable(),privileged,extension,orderByColumn,identColumn);			
		} finally {
			sc.setOptions(opts);
		}
	}
	
	// eventually we will be able to generate the user table stuff directly
	public void buildEntities(SchemaContext sc, CatalogSchema cs, CatalogDatabaseEntity db, int dmid, int storageid, 
			List<PersistedEntity> acc) throws PEException {
		if (view == InfoView.SHOW) return;
		String ct = String.format("create table %s ( %s )");
		List<Statement> stmts = InvokeParser.parse(ct,sc);
		PECreateTableStatement pecs = (PECreateTableStatement) stmts.get(0);
		ViewBasedInformationSchemaTableView.buildTableEntity(cs, db, dmid, storageid, acc, pecs.getTable());
	}
}
