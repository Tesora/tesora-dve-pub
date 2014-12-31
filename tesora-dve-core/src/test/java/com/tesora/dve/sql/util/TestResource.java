package com.tesora.dve.sql.util;

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

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;
import com.tesora.dve.db.DBNative;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.schema.SchemaContext;

public class TestResource {

	private ConnectionResource conn;
	private ProjectDDL ddl;
	private int connectionID;
	
	public TestResource(ConnectionResource cr, ProjectDDL ddl) {
		this(cr, ddl, -1);
	}
	
	public TestResource(ConnectionResource cr, ProjectDDL ddl, int connID) {
		conn = cr;
		this.ddl = ddl;
		this.connectionID = connID;
	}
	
	public ConnectionResource getConnection() {
		return conn;
	}
	
	public ProjectDDL getDDL() {
		return ddl;
	}
	
	public int getConnectionID() {
		return connectionID;
	}
	
	public void setConnectionID(int i) {
		connectionID = i;
	}
	
	public SchemaContext getContext() throws Exception {
		CatalogDAO cat = CatalogDAOFactory.newInstance();
		SchemaContext sc = SchemaContext.createContext(cat,
				Singletons.require(DBNative.class).getTypeCatalog());
		sc.setCurrentDatabase(sc.findDatabase(ddl.getDatabaseName()));
		return sc;
	}

	public void create() throws Throwable {
		getDDL().create(getConnection());
	}
	
	public void destroy() throws Throwable {
		getDDL().destroy(getConnection());
	}
}
