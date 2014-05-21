// OS_STATUS: public
package com.tesora.dve.sql.statement.ddl;

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

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.Container;
import com.tesora.dve.common.catalog.UserDatabase;
import com.tesora.dve.common.catalog.UserTable;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.lockmanager.LockType;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.schema.DatabaseLock;
import com.tesora.dve.sql.schema.PEDatabase;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.StatementType;

public class PEDropDatabaseStatement extends
		PEDropStatement<PEDatabase, UserDatabase> {

	private List<CatalogEntity> updates;
	private List<CatalogEntity> deletes;
	
	public PEDropDatabaseStatement(PEDatabase peds, String tag) {
		super(PEDatabase.class, null, false, peds, tag);
		updates = null;
		deletes = null;
	}
		
	@SuppressWarnings("unchecked")
	private void compute(SchemaContext sc) {
		if (deletes != null) return;
		
		DatabaseLock dl = new DatabaseLock("drop database",(PEDatabase)getTarget());
		sc.getConnection().acquireLock(dl, LockType.EXCLUSIVE);
		
		// for all tables in database that are base tables for a container:
		//   for all such containers:
		//      if all tables in container are in the target database - ok
		//          update container to remove base table
		//      otherwise - error
		//   
		CatalogDAO c = sc.getCatalog().getDAO();
		try {
			UserDatabase udb = getTarget().getPersistent(sc);
			List<UserTable> baseTables = c.findBaseTables(udb);
			deletes = new ArrayList<CatalogEntity>();
			deletes.add(udb);
			if (baseTables.isEmpty()) {
				updates = Collections.EMPTY_LIST;
			} else {
				updates = new ArrayList<CatalogEntity>();
				for(UserTable ut : baseTables) {
					Container ofContainer = ut.getContainer();
					List<UserDatabase> dbsOf = c.findDatabasesWithin(ofContainer);
					if (dbsOf.size() > 1)
						throw new SchemaException(Pass.PLANNER, "Unable to drop database " 
								+ getTarget().getName().getSQL() 
								+ " because table " + ut.getName() 
								+ " is a container base table for container " + ofContainer.getName());
					else {
						// remove the base table from the container
						ofContainer.setBaseTable(null);
						updates.add(ofContainer);
					}
				}
			}
		} catch (PEException pe) {
			throw new SchemaException(Pass.PLANNER, "Unable to compute drop set for drop database",pe);
		}
		
	}
	
	
	@Override
	public List<CatalogEntity> getCatalogObjects(SchemaContext sc) throws PEException {
		compute(sc);
		return updates;
	}
	
	@Override
	public List<CatalogEntity> getDeleteObjects(SchemaContext sc) throws PEException {
		compute(sc);
		return deletes;
	}

	
	@Override
	public StatementType getStatementType() {
		return StatementType.DROP_DB;
	}
		

}
