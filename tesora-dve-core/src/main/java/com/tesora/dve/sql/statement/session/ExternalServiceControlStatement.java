// OS_STATUS: public
package com.tesora.dve.sql.statement.session;

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

import com.tesora.dve.common.catalog.ExternalService;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.externalservice.ExternalServiceFactory;
import com.tesora.dve.externalservice.ExternalServicePlugin;
import com.tesora.dve.queryplan.QueryStepGeneralOperation.AdhocOperation;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.TransientSessionExecutionStep;
import com.tesora.dve.worker.WorkerGroup;

public class ExternalServiceControlStatement extends SessionStatement {

	public enum Action {
		START, STOP
	}

	Action action;
	ExternalService externalService;

	public ExternalServiceControlStatement(Action action) {
		this(action, null);
	}

	public ExternalServiceControlStatement(Action action, ExternalService es) {
		super();
		
		this.action = action;
		this.externalService = es;
	}

	@Override
	public void plan(SchemaContext sc, ExecutionSequence es) throws PEException {
		es.append(new TransientSessionExecutionStep(action.name() + " EXTERNAL SERVICE",
				new AdhocOperation() {

					@Override
					public void execute(SSConnection ssCon, WorkerGroup wg, DBResultConsumer resultConsumer)
							throws Throwable {
						ExternalServicePlugin plugin = null;
						boolean isRegistered = ExternalServiceFactory.isRegistered(externalService.getName());
						
						if ( isRegistered ) 
							plugin = ExternalServiceFactory.getInstance(externalService.getName());
						try {
							switch(action) {
							case START:
								if (!isRegistered) {
									plugin = ExternalServiceFactory.register(externalService.getName(), externalService.getPlugin());
								}
								if (plugin != null) {
									plugin.start();
								}
								break;
							case STOP:
								if ( isRegistered )
									plugin.stop();
								else
									throw new PEException("Cannot stop service '" + externalService.getName() + "' because it isn't registered");
								break;
							default:
								break;
							}
						} catch (Exception e) {
							throw new PEException("Unable to " + action.name() + " service "
									+ externalService.getName(), e);
						}
					}
				}));
	}

	public Action getAction() {
		return action;
	}
}