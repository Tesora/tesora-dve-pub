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


import java.util.List;

import com.tesora.dve.db.DBNative;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.persist.PersistedEntity;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.infoschema.annos.InfoView;
import com.tesora.dve.sql.infoschema.engine.ViewQuery;
import com.tesora.dve.sql.infoschema.persist.CatalogDatabaseEntity;
import com.tesora.dve.sql.infoschema.persist.CatalogSchema;
import com.tesora.dve.sql.schema.Name;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.Table;
import com.tesora.dve.sql.statement.dml.SelectStatement;

// strip the type info off for now
public interface InformationSchemaTable extends Table<InformationSchemaColumn> {


	public InfoView getView();
	
	public void prepare(AbstractInformationSchema view, DBNative dbn);
	
	public void inject(AbstractInformationSchema view, DBNative dbn);

	public void freeze();

	// views have no backing logical table
	public LogicalInformationSchemaTable getLogicalTable();

	public void buildTableEntity(CatalogSchema cs, CatalogDatabaseEntity db, int dmid, int storageid, List<PersistedEntity> acc) throws PEException;

	public InformationSchemaColumn getOrderByColumn();
	
	public InformationSchemaColumn getIdentColumn();

	public boolean requiresPriviledge();
	
	public boolean isExtension();
	
	public void annotate(SchemaContext sc, ViewQuery vq, SelectStatement in, TableKey onTK);

	public boolean isVariablesTable();

	public void assertPermissions(SchemaContext sc);

	public boolean isView();
	
	public Name getPluralName();
}
