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

import java.util.HashSet;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.ValueManager;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.NestedExecutionPlan;
import com.tesora.dve.sql.transform.strategy.PlannerContext;

public class NestedPlanFeatureStep extends FeatureStep {

	private final FeatureStep nested;
	private final ValueManager vm;
	
	public NestedPlanFeatureStep(FeatureStep targ,ValueManager vm) {
		super(targ.planners.get(0),targ.group,targ.distributionKey);
		this.nested = targ;
		this.vm = vm;
		withDefangInvariants();
	}

	@Override
	protected void scheduleSelf(PlannerContext sc, ExecutionSequence es)
			throws PEException {
		// so for this we build a new nested plan and add that to the sequence
		// and also register ourselves with the parent plan
		NestedExecutionPlan nep = new NestedExecutionPlan(vm);
		nested.schedule(sc, nep.getSequence(), new HashSet<FeatureStep>());
		es.append(nep);
		es.getPlan().addNestedPlan(nep);
	}

	@Override
	public DMLStatement getPlannedStatement() {
		return nested.getPlannedStatement();
	}

	@Override
	public Database<?> getDatabase(PlannerContext pc) {
		return nested.getDatabase(pc);
	}

}
