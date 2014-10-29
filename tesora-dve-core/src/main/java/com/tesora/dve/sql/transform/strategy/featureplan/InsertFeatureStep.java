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
import com.tesora.dve.sql.schema.DistributionKey;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.statement.session.TransactionStatement;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.ExecutionStep;
import com.tesora.dve.sql.transform.execution.InsertExecutionStep;
import com.tesora.dve.sql.transform.execution.TransactionExecutionStep;
import com.tesora.dve.sql.transform.strategy.PlannerContext;

public abstract class InsertFeatureStep extends FeatureStep {

	protected final boolean requiresReferenceTimestamp;
	protected final Boolean cacheable;
	protected final TransactionStatement.Kind txnal;

	public InsertFeatureStep(FeaturePlanner planner, PEStorageGroup srcGroup,
			DistributionKey key, boolean reqTimestamp, Boolean isCacheable, TransactionStatement.Kind txnal) {
		super(planner, srcGroup, key);
		requiresReferenceTimestamp = reqTimestamp;
		this.cacheable = isCacheable;
		this.txnal = txnal;
		withDefangInvariants();
	}

	@Override
	protected final void scheduleSelf(PlannerContext sc, ExecutionSequence es)
			throws PEException {
		ExecutionStep step = buildStep(sc,es);
		if (step instanceof InsertExecutionStep) {
			InsertExecutionStep ies = (InsertExecutionStep) step;
			ies.setRequiresReferenceTimestamp(requiresReferenceTimestamp);
		}
		if (es.getPlan() != null) {
			if (Boolean.FALSE.equals(cacheable))
				es.getPlan().setCacheable(false);
			else
				es.getPlan().setCacheable(true);
		}
		if (txnal == TransactionStatement.Kind.START)
			es.append(TransactionExecutionStep.buildStart(sc.getContext(), getDatabase(sc)));
		es.append(step);
		if (txnal == TransactionStatement.Kind.COMMIT)
			es.append(TransactionExecutionStep.buildCommit(sc.getContext(), getDatabase(sc)));
	}

	protected abstract ExecutionStep buildStep(PlannerContext sc, ExecutionSequence es) throws PEException;
	
}
