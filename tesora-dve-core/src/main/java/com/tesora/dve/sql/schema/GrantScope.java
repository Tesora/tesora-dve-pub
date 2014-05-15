// OS_STATUS: public
package com.tesora.dve.sql.schema;

import com.tesora.dve.sql.schema.mt.PETenant;

public class GrantScope {

	private final PEDatabase db;
	private final PETenant tenant;
	private final Name nameOnly;
	
	public GrantScope(PEDatabase pdb) {
		db = pdb;
		tenant = null;
		nameOnly = null;
	}
	
	public GrantScope(PETenant ten) {
		db = null;
		tenant = ten;
		nameOnly = null;
	}
	
	public GrantScope(Name nn) {
		db = null;
		tenant = null;
		nameOnly = nn;
	}
	
	public GrantScope() {
		db = null;
		tenant = null;
		nameOnly = null;
	}
	
	public PEDatabase getDatabase() {
		return db;
	}
	
	public PETenant getTenant() {
		return tenant;
	}
	
	public PEPriviledge buildPriviledge(SchemaContext pc, PEUser u) {
		if (db != null)
			return new PEPriviledge(u,db);
		else
			return new PEPriviledge(u,tenant);
	}
	
	public String getSQL() {
		if (db != null)
			return db.getName().getSQL() + ".*";
		else if (tenant != null)
			return tenant.getName().getSQL() + ".*";
		else if (nameOnly != null)
			return nameOnly.getSQL();
		else
			return "*.*";
	}
}
