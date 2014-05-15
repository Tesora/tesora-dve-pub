// OS_STATUS: public
package com.tesora.dve.sql.util;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;
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
		SchemaContext sc = SchemaContext.createContext(cat);
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
