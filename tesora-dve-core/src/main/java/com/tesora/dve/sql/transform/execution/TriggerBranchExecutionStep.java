package com.tesora.dve.sql.transform.execution;

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

import com.tesora.dve.common.PEStringUtils;
import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.queryplan.QueryStepOperation;
import com.tesora.dve.queryplan.QueryStepTriggerBranchOperation;
import com.tesora.dve.resultset.ProjectionInfo;
import com.tesora.dve.sql.schema.Database;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;

public class TriggerBranchExecutionStep extends ExecutionStep {

	private final HasPlanning caseEvaluationStep;
	private final List<HasPlanning> branchOperationSteps;

	public TriggerBranchExecutionStep(final Database<?> db, final PEStorageGroup storageGroup,
			final HasPlanning caseEvaluationStep,
			final List<HasPlanning> branchOperationSteps) {
		super(db, storageGroup, ExecutionType.TRIGGER);

		this.caseEvaluationStep = caseEvaluationStep;
		this.branchOperationSteps = branchOperationSteps;
	}

	@Override
	public void schedule(final ExecutionPlanOptions opts, final List<QueryStepOperation> qsteps,
			final ProjectionInfo projection, final SchemaContext sc, final ConnectionValuesMap cvm, final ExecutionPlan containing) throws PEException {
		final QueryStepOperation caseEvaluationOperation = buildOperation(opts, sc, cvm, containing, this.caseEvaluationStep);
		final List<QueryStepOperation> branchOperations = new ArrayList<QueryStepOperation>(this.branchOperationSteps.size());
		for (final HasPlanning step : this.branchOperationSteps) {
			branchOperations.add(buildOperation(opts, sc, cvm, containing, step));
		}

		qsteps.add(new QueryStepTriggerBranchOperation(caseEvaluationOperation, branchOperations));
	}

	@Override
	public void display(final SchemaContext sc, final ConnectionValuesMap cvm, final ExecutionPlan containingPlan, final List<String> buf, final String indent,
			final EmitOptions opts) {
		super.display(sc, cvm, containingPlan, buf, indent, opts);
		final String sub1 = PEStringUtils.getIndented(indent);
		final String sub2 = PEStringUtils.getIndented(indent, 2);
		buf.add(sub1 + "Branch evaluation");
		this.caseEvaluationStep.display(sc, cvm, containingPlan, buf, indent, opts);
		buf.add(sub1 + "Branches by index");
		final int numBranches = this.branchOperationSteps.size();
		for (int branchIndex = 0; branchIndex < numBranches; ++branchIndex) {
			final List<String> lines = new ArrayList<String>(numBranches);
			final HasPlanning step = this.branchOperationSteps.get(branchIndex);
			step.display(sc, cvm, containingPlan, lines, sub2, opts);
			buf.addAll(prependIndexToFirst(branchIndex, sub2, lines));
		}
	}

	private List<String> prependIndexToFirst(final int index, final String indent, final List<String> lines) {
		if (!lines.isEmpty()) {
			final String firstLine = lines.get(0).substring(indent.length());
			lines.set(0, indent.concat("[").concat(String.valueOf(index)).concat("] ").concat(firstLine));
		}

		return lines;
	}

}
