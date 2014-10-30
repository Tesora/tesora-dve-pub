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
import com.tesora.dve.sql.schema.DistributionKey;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.InsertIntoValuesStatement;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.execution.ExecutionStep;
import com.tesora.dve.sql.transform.execution.InsertExecutionStep;
import com.tesora.dve.sql.transform.strategy.PlannerContext;

public class InsertValuesFeatureStep extends InsertFeatureStep {

	private InsertIntoValuesStatement planned;
	private UpdateCountAdjuster adjuster = defaultAdjuster;
		
	public InsertValuesFeatureStep(InsertIntoValuesStatement planned, 
			FeaturePlanner planner, PEStorageGroup srcGroup,
			DistributionKey key,
			boolean refTimestamp,
			UpdateCountAdjuster adjuster) {
		super(planner, srcGroup, key, refTimestamp, planned.isCacheable(), planned.getTxnFlag());
		this.planned = planned;
		if (adjuster != null)
			this.adjuster = adjuster;
	}

	@Override
	protected ExecutionStep buildStep(PlannerContext sc, ExecutionSequence es)
			throws PEException {
		return InsertExecutionStep.build(sc.getContext(), getDatabase(sc), getSourceGroup(),
				planned,planned.getTable().asTable(),getDistributionKey(),adjuster.adjustUpdateCount(planned.getValues().size()));
	}

	
	@Override
	public DMLStatement getPlannedStatement() {
		return planned;
	}

	@Override
	public Database<?> getDatabase(PlannerContext pc) {
		return planned.getDatabase(pc.getContext());
	}

	public interface UpdateCountAdjuster {
		
		public Long adjustUpdateCount(int raw);
		
	}

	private static final UpdateCountAdjuster defaultAdjuster = new UpdateCountAdjuster() {

		public Long adjustUpdateCount(int in) {
			return new Long(in);
		}

	};

	
}
