// OS_STATUS: public
package com.tesora.dve.queryplan;

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
import java.util.List;

import org.apache.log4j.Logger;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogEntity;
import com.tesora.dve.common.catalog.PersistentDatabase;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.schema.cache.CacheInvalidationRecord;
import com.tesora.dve.worker.WorkerGroup;

public class QueryStepDDLOperation extends QueryStepDDLGeneralOperation {

	static Logger logger = Logger.getLogger( QueryStepSelectAllOperation.class );
	
	private StaticEntityGenerator catalogEntities;

	public QueryStepDDLOperation(PersistentDatabase db, SQLCommand command, CacheInvalidationRecord invalidationRecord) {
		super(db);
		catalogEntities = new StaticEntityGenerator(command,invalidationRecord);
		setEntities(catalogEntities);
	}
	
	public void addCatalogUpdate(CatalogEntity catEntity) {
		catalogEntities.addUpdate(catEntity);
	}

	public void addCatalogDeletion(CatalogEntity catEntity) {
		catalogEntities.addDelete(catEntity);
	}

	static class StaticEntityGenerator implements DDLCallback {

		private List<CatalogEntity> updates;
		private List<CatalogEntity> deletes;
		private SQLCommand commands;
		private CacheInvalidationRecord invalidation;
		
		StaticEntityGenerator(SQLCommand cmd, CacheInvalidationRecord cir) {
			updates = new ArrayList<CatalogEntity>();
			deletes = new ArrayList<CatalogEntity>();
			commands = cmd;
			invalidation = cir;
		}
		
		void addUpdate(CatalogEntity ce) {
			updates.add(ce);
		}
		
		void addDelete(CatalogEntity ce) {
			deletes.add(ce);
		}
		
		
		@Override
		public List<CatalogEntity> getUpdatedObjects() {
			return updates;
		}

		@Override
		public List<CatalogEntity> getDeletedObjects() {
			return deletes;
		}

		@Override
		public SQLCommand getCommand(CatalogDAO c) {
			return commands;
		}

		@Override
		public boolean canRetry(Throwable t) {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public String description() {
			return System.identityHashCode(this) + "@StaticEntityGenerator: {" + commands + "}";
		}

		@Override
		public CacheInvalidationRecord getInvalidationRecord() {
			return invalidation;
		}

		@Override
		public void beforeTxn(SSConnection ssConn, CatalogDAO c, WorkerGroup wg) throws PEException {
		}

		@Override
		public void inTxn(SSConnection conn, WorkerGroup wg) throws PEException {
		}

		@Override
		public boolean requiresFreshTxn() {
			return false;
		}

		@Override
		public boolean requiresWorkers() {
			return !commands.isEmpty();
		}

		@Override
		public void postCommitAction(CatalogDAO c) {
			// TODO Auto-generated method stub
			
		}
		
		
	}
}
