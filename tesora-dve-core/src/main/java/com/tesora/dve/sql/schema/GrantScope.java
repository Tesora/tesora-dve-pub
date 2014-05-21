package com.tesora.dve.sql.schema;

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
