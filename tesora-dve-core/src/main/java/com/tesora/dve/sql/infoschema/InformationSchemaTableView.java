package com.tesora.dve.sql.infoschema;


import java.util.List;

import com.tesora.dve.db.DBNative;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.persist.PersistedEntity;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.engine.ViewQuery;
import com.tesora.dve.sql.infoschema.persist.CatalogDatabaseEntity;
import com.tesora.dve.sql.infoschema.persist.CatalogSchema;
import com.tesora.dve.sql.schema.Column;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.Table;
import com.tesora.dve.sql.statement.dml.SelectStatement;

// strip the type info off for now
public interface InformationSchemaTableView extends Table {


	public InfoView getView();
	
	public void prepare(SchemaView view, DBNative dbn);
	
	public void inject(SchemaView view, DBNative dbn);

	public void freeze();

	// views have no backing logical table
	public LogicalInformationSchemaTable getLogicalTable();

	public void buildTableEntity(CatalogSchema cs, CatalogDatabaseEntity db, int dmid, int storageid, List<PersistedEntity> acc) throws PEException;

	public Column getOrderByColumn();
	
	public Column getIdentColumn();

	public boolean requiresPriviledge();
	
	public boolean isExtension();
	
	public void annotate(SchemaContext sc, ViewQuery vq, SelectStatement in, TableKey onTK);

	public boolean isVariablesTable();

	public void assertPermissions(SchemaContext sc);

	public boolean isView();
}
