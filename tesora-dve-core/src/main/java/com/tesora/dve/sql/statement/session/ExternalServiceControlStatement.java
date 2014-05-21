// OS_STATUS: public
package com.tesora.dve.sql.statement.session;

import com.tesora.dve.common.catalog.ExternalService;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.externalservice.ExternalServiceFactory;
import com.tesora.dve.externalservice.ExternalServicePlugin;
import com.tesora.dve.queryplan.QueryStepGeneralOperation.AdhocOperation;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
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
	public void plan(SchemaContext sc, ExecutionSequence es, BehaviorConfiguration config) throws PEException {
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