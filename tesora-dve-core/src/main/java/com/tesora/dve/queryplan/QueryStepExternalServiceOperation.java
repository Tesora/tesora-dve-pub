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

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.PersistentDatabase;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.externalservice.ExternalServiceFactory;
import com.tesora.dve.externalservice.ExternalServicePlugin;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.server.messaging.SQLCommand;
import com.tesora.dve.sql.schema.PEExternalService;
import com.tesora.dve.sql.transform.execution.CatalogModificationExecutionStep.Action;
import com.tesora.dve.worker.WorkerGroup;

public class QueryStepExternalServiceOperation extends QueryStepDDLOperation {

	Action action;
	PEExternalService rootEntity;

	public QueryStepExternalServiceOperation(PersistentDatabase execCtxDBName,
			SQLCommand command, Action action, PEExternalService rootEntity2) {
		super(execCtxDBName, command,null);

		this.action = action;
		this.rootEntity = rootEntity2;
	}

	@Override
	protected void prepareAction(CatalogDAO c) throws PEException {
		super.prepareAction(c);
		
		switch(action) {
		case DROP:
		{
			if ( ExternalServiceFactory.isRegistered(rootEntity.getExternalServiceName())) {
				ExternalServicePlugin plugin = ExternalServiceFactory.getInstance(rootEntity.getExternalServiceName());
				plugin.stop();
			}
			
			// do this all the time
			ExternalServiceFactory.deregister(rootEntity.getExternalServiceName());
			break;
		}
		case ALTER:
		{
			// Alter is allowed if the service isn't currently registered, so check for that
			if ( ExternalServiceFactory.isRegistered(rootEntity.getExternalServiceName())) {
				ExternalServicePlugin plugin = ExternalServiceFactory.getInstance(rootEntity.getExternalServiceName());
				if (plugin.isStarted()) {
					throw new PEException("Cannot change external service while service is started.");
				}
			}
			break;
		}
		default :
			break;
		}
	}
	
	@Override
	public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer)
			throws Throwable {
		// call base first to get the external service into the catalog
		super.execute(ssCon, wg, resultConsumer);

		// we do this here instead of prepareAction because we need the service
		// to be committed (by super.execute) to the catalog before registering 
		// and starting it
		switch(action) {
		case CREATE:
			// no need to check if service exists in map already
//			ExternalServiceContextImpl ctxt = new ExternalServiceContextImpl(
//					rootEntity.getExternalServiceName());
//			ExternalServicePlugin plugin = ExternalServiceFactory.register(ctxt,
//					rootEntity.getPlugin(), rootEntity.getExternalServiceName());
//			if (ctxt.getServiceAutoStart()) {
//				plugin.start();
//			}
			ExternalServiceFactory.register(rootEntity.getExternalServiceName(), rootEntity.getPlugin());
			
			break;
			
		case ALTER:
			// Alter is allowed if the service isn't registered
			if ( ExternalServiceFactory.isRegistered(rootEntity.getExternalServiceName())) {
				ExternalServicePlugin pi = ExternalServiceFactory.getInstance(rootEntity.getExternalServiceName());
				pi.reload();
			}
			break;
			
		default :
			break;
		}
	}
}
