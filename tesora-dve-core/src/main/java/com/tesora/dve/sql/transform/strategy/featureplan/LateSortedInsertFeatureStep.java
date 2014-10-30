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
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.InsertIntoValuesStatement;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.ExecutionStep;
import com.tesora.dve.sql.transform.execution.LateSortingInsertExecutionStep;
import com.tesora.dve.sql.transform.strategy.PlannerContext;

public class LateSortedInsertFeatureStep extends InsertFeatureStep {

	private final InsertIntoValuesStatement planned;
	
	public LateSortedInsertFeatureStep(InsertIntoValuesStatement iivs, FeaturePlanner planner,
			PEStorageGroup srcGroup, boolean reqTimestamp) {
		super(planner, srcGroup, null, reqTimestamp, iivs.isCacheable(),iivs.getTxnFlag());
		this.planned = iivs;
	}

	@Override
	public DMLStatement getPlannedStatement() {
		return planned;
	}

	@Override
	public Database<?> getDatabase(PlannerContext pc) {
		return planned.getDatabase(pc.getContext());
	}

	@Override
	protected ExecutionStep buildStep(PlannerContext sc, ExecutionSequence es)
			throws PEException {
		return LateSortingInsertExecutionStep.build(sc.getContext(), getDatabase(sc), 
				getSourceGroup(),
				planned.getTable().asTable(),
				(planned.getOnDuplicateKeyEdge().size() > 0 || planned.getIgnore()));
	}

}
