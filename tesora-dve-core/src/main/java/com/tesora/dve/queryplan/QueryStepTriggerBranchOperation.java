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

import com.tesora.dve.db.DBResultConsumer;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.worker.MysqlTextResultCollector;
import com.tesora.dve.worker.WorkerGroup;

public class QueryStepTriggerBranchOperation extends QueryStepOperation {

	private final QueryStepOperation branchEvaluation;
	private final List<QueryStepOperation> branchOperations;

	private QueryStepOperation targetOperation;

	public QueryStepTriggerBranchOperation(final QueryStepOperation branchEvaluation, final List<QueryStepOperation> branchOperations) throws PEException {
		super(branchEvaluation.getStorageGroup());
		this.branchEvaluation = branchEvaluation;
		this.branchOperations = branchOperations;
	}

	@Override
	public void execute(final ExecutionState estate, final DBResultConsumer resultConsumer) throws Throwable {
		executeRequirements(estate);

		final MysqlTextResultCollector branchConditionEvalResult = new MysqlTextResultCollector(false);
		this.branchEvaluation.execute(estate, branchConditionEvalResult); // Get index of the executed branch.

		// There should be exactly one value - index of the target branch.
		final List<ArrayList<String>> targetBranchIndices = branchConditionEvalResult.getRowData();
		if (targetBranchIndices.size() == 1) {
			final ArrayList<String> indexValues = targetBranchIndices.get(0);
			if (indexValues.size() == 1) {
				final int branchIndex = Integer.parseInt(indexValues.get(0));
				this.targetOperation = this.branchOperations.get(branchIndex);
				return;
			}
		}

		throw new PEException("Unexpected branch index in trigger CASE evaluation.");
	}


	@Override
	public void executeSelf(final ExecutionState estate, final WorkerGroup wg, final DBResultConsumer resultConsumer) throws Throwable {
		this.targetOperation.executeSelf(estate, wg, resultConsumer);
	}

}
