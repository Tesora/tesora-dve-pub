// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.featureplan;

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

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.DistributionKey;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.TempTable;
import com.tesora.dve.sql.schema.TempTableCreateOptions;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.transform.execution.DMLExplainRecord;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.ProjectingExecutionStep;
import com.tesora.dve.sql.transform.strategy.ExecutionCost;
import com.tesora.dve.sql.transform.strategy.PlannerContext;

public class ProjectingFeatureStep extends FeatureStep {

	private final ProjectingStatement stmt;
	private final ExecutionCost cost;
	private final DistributionVector distVect;
	private final Database<?> db;
	private LiteralExpression inMemLimit;
	
	public ProjectingFeatureStep(PlannerContext pc, FeaturePlanner planner, ProjectingStatement statement, ExecutionCost cost, PEStorageGroup group, DistributionKey dk, Database<?> db, DistributionVector vector) {
		super(planner, group, dk);
		this.stmt = statement;
		this.cost = cost;
		this.db = db;
		this.distVect = vector;
	}
	
	public DistributionVector getDistributionVector() {
		return distVect;
	}
	
	@Override
	public Database<?> getDatabase(PlannerContext pc) {
		return db;
	}

	public ExecutionCost getCost() {
		return cost;
	}
	
	@Override
	public void scheduleSelf(PlannerContext pc, ExecutionSequence es)
			throws PEException {
		ProjectingExecutionStep pes =
				ProjectingExecutionStep.build(
						pc.getContext(), 
						db,
						getSourceGroup(),
						distVect,
						getDistributionKey(),
						stmt,
						getExplainRecord());
		if (inMemLimit != null)
			pes.setInMemoryLimit(inMemLimit);
		es.append(pes);		
	}

	// build a new redist step out of this proj step, using the given opts
	public RedistFeatureStep redist(PlannerContext pc, FeaturePlanner onBehalfOf,
			TempTableCreateOptions createOpts,
			RedistributionFlags flags,
			DMLExplainRecord redistExplain) throws PEException {
		TempTable tt = TempTable.build(pc.getContext(), (ProjectingStatement)getPlannedStatement(), createOpts);
		TableKey tk = new TableKey(tt, 0);
		RedistFeatureStep redist = new RedistFeatureStep(onBehalfOf, this,tk, createOpts.getGroup(), createOpts.getDistVectColumns(), flags);
		redist.withExplain(redistExplain == null ? getExplainRecord() : redistExplain);
		return redist;
	}
		
	public TempTable getSourceTempTable() {
		if (getSelfChildren().isEmpty()) return null;
		if (getSelfChildren().get(0) instanceof RedistFeatureStep) {
			RedistFeatureStep rfs = (RedistFeatureStep) getSelfChildren().get(0);
			TableKey targetTab = rfs.getTargetTable();
			if (targetTab.getAbstractTable().isTempTable())
				return (TempTable)targetTab.getAbstractTable();
		}
		return null;
	}
	
	
	@Override
	public DMLStatement getPlannedStatement() {
		return stmt;
	}
	
	public void setInMemLimit(LiteralExpression litex) {
		inMemLimit = litex;
	}
	
}
