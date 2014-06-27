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

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.log4j.Logger;

import com.tesora.dve.common.PEThreadContext;
import com.tesora.dve.common.catalog.PersistentGroup;
import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.common.logutil.ExecutionLogger;
import com.tesora.dve.db.DBEmptyTextResultConsumer;
import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.eventing.SynchronousEventHandler;
import com.tesora.dve.eventing.events.QSOExecuteRequestEvent;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.resultset.collector.ResultCollector;
import com.tesora.dve.server.connectionmanager.SSConnection;
import com.tesora.dve.worker.WorkerGroup;

/**
 * <b>QueryStep</b> represents the components of a step in a {@link QueryPlan}.
 * <p/>
 * Each <b>QueryStep</b> is composed of three operations: an <em>execute</em> operation, plus
 * optional <em>setup</em> and <em>cleanup</em> operations.  The <em>setup</em> operation is
 * executed immediately before the <em>execute</em> operation, while the <em>cleanup</em> operation
 * can be executed at any time, possibly as late as {@link QueryPlan#close()}.  All operations are executed
 * against the same {@link PersistentGroup}.
 * <p/>
 * Typical <b>QueryStep</b> operations are {@link QueryStepInsertByKeyOperation},
 * {@link QueryStepSelectAllOperation} or {@link QueryStepRedistOperation}, though
 * there may be others.
 * <p/>
 * Each <b>QueryStep</b> may also have zero or more dependencies.  Dependencies
 * are themselves instances of <b>QueryStep</b>, with their own <em>setup</em>,
 * <em>execute</em> and <em>cleanup</em> operations and their own dependencies.
 * Dependencies are executed before the <em>execute</em> operation, but may be 
 * executed before, during, or after the <em>setup</em> operation.
 * 		
 */
@XmlRootElement(name="QueryStep")
public class QueryStep {
	
	Logger logger = Logger.getLogger(QueryStep.class);
	
	@XmlElement(name="StorageGroup")
	StorageGroup storageGroup;
	
	@XmlElement(name="SetupOperation")
	QueryStepOperation setupOperation = null;
	@XmlElement(name="ExecuteOperation")
	QueryStepOperation executeOperation = null;
	@XmlElement(name="CleanupOperation")
	QueryStepDropTempTableOperation cleanupOperation = null;

	@XmlElement(name="DependencyStep")
	List<QueryStep> dependencySteps = new ArrayList<QueryStep>();

	private boolean needsCleanup = false;
	
	public QueryStep() {
	}
	
	/**
	 * Constructs a <b>QueryStep</b> with operations against the provided {@link PersistentGroup}.
	 * 
	 * @param storageGroup2 the {@link PersistentGroup} against which to execute the operations
	 * @param exec the "workhorse" operation
	 */
	public QueryStep(StorageGroup storageGroup2, QueryStepOperation exec) {
		storageGroup = storageGroup2;
		executeOperation = exec;
	}
	
	/**
	 * Executes this step in the {@link QueryPlan}.
	 * <p/>
	 * First, the <em>setup</em> operation and any dependency steps are executed, then
	 * the <em>execute</em> step is executed and any results are returned, if any.
	 * @param resultConsumer 
	 * 
	 * @param ssConnection the user's connection
	 * @return {@link ResultCollector} with results
	 * @throws Throwable 
	 */
	public void executeStep(SSConnection ssCon, DBResultConsumer resultConsumer) throws Throwable  {
		PEThreadContext.pushFrame(getClass().getSimpleName()).put("storageGroup", storageGroup).logDebug();
		try {
			validateStep();
			needsCleanup = true;
			doExecuteStep(ssCon, resultConsumer);
		} finally {
			PEThreadContext.popFrame();
		}
	}

	void doExecuteStep(SSConnection ssCon, DBResultConsumer resultConsumer) throws Throwable {
		executeDependents(ssCon);
		executeQueryOperation(executeOperation, ssCon, resultConsumer);
	}
	
	/**
	 * Adds a setup operation to the <b>QueryStep</b>.
	 * 
	 * @param setup operation to add
	 * @return itself for inline calls
	 */
	public QueryStep addSetupOperation(QueryStepOperation setup) {
		setupOperation = setup;
		return this;
	}
	
	/**
	 * Adds a cleanup operation to the <b>QueryStep</b>.
	 * 
	 * @param cleanup operation to add
	 * @return itself for inline calls
	 */
	public QueryStep addCleanupOperation(QueryStepDropTempTableOperation cleanup) {
//		cleanupOperation = cleanup;
		return this;
	}
	
	private void validateStep() throws PEException {
		if (executeOperation == null)
			throw new PEException("QueryStep has no Execute operation");
	}

	/**
	 * Executes the <em>cleanup</em> steps of any dependency steps,
	 * then its own <em>cleanup</em> step.
	 * 
	 * @param ssConnection the user's connection
	 * @throws Throwable 
	 */
	public void cleanupStep(SSConnection ssCon) throws Throwable {
		if (needsCleanup ) {
			for (QueryStep step : dependencySteps) {
				step.cleanupStep(ssCon);
			}
//			if (cleanupOperation != null) {
//				WorkerGroup wg = null;
//				ResultCollector rc = null;
//				try {
//					wg = new WorkerGroup(storageGroup).provision(ssCon, null, ssCon.getUserAuthentication());
//					rc = cleanupOperation.execute(ssCon, wg);
//				} finally {
//					if (wg != null)
//						wg.releaseWorkers(ssCon);
//					ResultCollectorFactory.returnInstance(rc);
//				}
//			}
		}
	}
	
	/**
	 * Adds a dependency step to the <b>QueryStep</b>.  Dependency steps
	 * will be executed in the order in which they are added.
	 * 
	 * @param step {@link QueryStep} to add as dependency
	 * @return itself for inline execution
	 */
	public QueryStep addDependencyStep(QueryStep step) {
		dependencySteps.add(step);
		return this;
	}

	void executeDependents(SSConnection ssCon) throws Throwable {
		executeQueryOperation(setupOperation, ssCon, DBEmptyTextResultConsumer.INSTANCE);
		for (QueryStep step : dependencySteps) {
			step.executeStep(ssCon, DBEmptyTextResultConsumer.INSTANCE);
		}
	}
	
	void executeQueryOperation(QueryStepOperation op, SSConnection ssCon, DBResultConsumer resultConsumer) throws Throwable {
		if (op!= null) {
			PEThreadContext.pushFrame("QueryStepOperation")
					.put("resultConsumer", resultConsumer)
					.put("operation", op.describeForLog())
					.logDebug();
			try {
				ExecutionLogger beforeLogger = ssCon.getExecutionLogger().getNewLogger("BeforeStepExec");
				WorkerGroup wg = null;
				if (op.requiresImplicitCommit())
					ssCon.implicitCommit();
				if (op.requiresWorkers()) {
					wg = ssCon.getWorkerGroupAndPushContext(storageGroup,op.getContextDatabase());
				}
				ExecutionLogger slowQueryLogger = null;
				try {
					beforeLogger.end();
					if (logger.isDebugEnabled())
						logger.debug("QueryStep executes " + op.toString());
					slowQueryLogger = ssCon.getExecutionLogger().getNewLogger(op); 

					SynchronousEventHandler sync = new SynchronousEventHandler();
					QSOExecuteRequestEvent execRequest = new QSOExecuteRequestEvent(sync,ssCon,wg,resultConsumer);
					sync.call(execRequest, op);
					
					// op.execute(ssCon, wg, resultConsumer);
				} finally {
					ExecutionLogger afterLogger = null;
					try {
						slowQueryLogger.end();
						afterLogger = ssCon.getExecutionLogger().getNewLogger("AfterStepExec");
					} finally {
						try {
							if (wg != null)
								ssCon.returnWorkerGroupAndPopContext(wg);
						} finally {
							if (afterLogger != null)
								afterLogger.end();
						}
					}
				}
			} finally {
				PEThreadContext.popFrame();
			}
		}
	}
	
	public QueryStepOperation getOperation() {
		return executeOperation;
	}
	
	boolean requiresTransaction() {
		boolean tolerates = false;
		if (executeOperation != null)
			tolerates = executeOperation.requiresTransaction();
		for (QueryStep step : dependencySteps) {
			tolerates = tolerates || step.requiresTransaction();
		}
		return tolerates;
	}
	
	boolean hasDependents() {
		return dependencySteps.isEmpty() == false;
	}
	
	public StorageGroup getSourceGroup() {
		return storageGroup;
	}
}
