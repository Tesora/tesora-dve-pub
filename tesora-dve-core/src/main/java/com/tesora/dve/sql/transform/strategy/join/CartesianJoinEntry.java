// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.join;

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
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.expression.RewriteKey;
import com.tesora.dve.sql.jg.DGJoin;
import com.tesora.dve.sql.jg.DPart;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.DistributionVector.Model;
import com.tesora.dve.sql.schema.TempTableCreateOptions;
import com.tesora.dve.sql.statement.dml.DMLStatementUtils;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.execution.DMLExplainReason;
import com.tesora.dve.sql.transform.execution.DMLExplainRecord;
import com.tesora.dve.sql.transform.strategy.ExecutionCost;
import com.tesora.dve.sql.transform.strategy.featureplan.ProjectingFeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.RedistFeatureStep;
import com.tesora.dve.sql.transform.strategy.join.JoinRewriteTransformFactory.JoinRewriteAdaptedTransform;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.UnaryFunction;

/*
 * Used for any join that doesn't have equijoins in the join condition
 */
public class CartesianJoinEntry extends BinaryJoinEntry {

	public CartesianJoinEntry(SchemaContext sc, SelectStatement orig, 
			OriginalPartitionEntry lp, OriginalPartitionEntry rp, JoinRewriteAdaptedTransform jrat,
			JoinRewriteTransformFactory factory) {
		super(sc, orig, lp, rp, jrat,factory);
	}

	@Override
	public DGJoin getJoin() {
		return null;
	}
	
	@Override
	public JoinedPartitionEntry schedule()
			throws PEException {
		return build(getLeftPartition(), getRightPartition());
	}

	@Override
	public JoinedPartitionEntry schedule(JoinedPartitionEntry head) throws PEException {
		Set<DPart> scheduledPartitions = head.getPartitions();
		PartitionEntry otherSide = null;
		if (scheduledPartitions.contains(getLeftPartition().getPartition())) {
			// join head to right
			otherSide = getRightPartition();
		} else {
			otherSide = getLeftPartition();
		}
		return build(head,otherSide);
	}

	@Override
	public JoinedPartitionEntry schedule(List<JoinedPartitionEntry> lipes) throws PEException {
		if (lipes.size() != 2)
			throw new PEException("Cartesian join entry scheduled with more than two joins");
		return build(lipes.get(0),lipes.get(1));
	}

	private JoinedPartitionEntry build(PartitionEntry apart, PartitionEntry bpart) throws PEException {
		PartitionEntry[] sizedEntries = new PartitionEntry[2];
		if (apart.getScore().compareTo(bpart.getScore()) < 0) {
			sizedEntries[0] = apart;
			sizedEntries[1] = bpart;
		} else {
			sizedEntries[0] = bpart;
			sizedEntries[1] = apart;
		}

		Model aModel = null;
		Model bModel = null;
		
		PEStorageGroup targetGroup = null;

		ExecutionCost combinedCost =
				combineScores(getSchemaContext(), apart.getScore(), bpart.getScore(), null);
		
		if (sizedEntries[0].getScore().getConstraint() != null) {
			// can redist back onto the persistent group, or wherever the larger entry exists
			targetGroup = sizedEntries[1].getSourceGroup();
			if (apart == sizedEntries[0])
				aModel = Model.BROADCAST;
			else
				bModel = Model.BROADCAST;
			// no static entry
		} else {
			targetGroup = getPlannerContext().getTempGroupManager().getGroup(combinedCost.getGroupScore());
			if (apart == sizedEntries[0]) {
				aModel = Model.BROADCAST;
				bModel = Model.STATIC;
			} else {
				aModel = Model.STATIC;
				bModel = Model.BROADCAST;
			}
		}

		ListSet<DistributionVector> vectors = new ListSet<DistributionVector>();
		ArrayList<PartitionEntry> components = new ArrayList<PartitionEntry>();

		SelectStatement aSelect = null;
		SelectStatement bSelect = null;

		if (bModel != null) {
			RedistFeatureStep tempTab =
					redist(bpart,bModel,targetGroup);
			bpart.setStep(tempTab);
			bSelect = tempTab.buildNewSelect(getPlannerContext());
			vectors.add(tempTab.getTargetTempTable().getDistributionVector(getSchemaContext()));
		} else {
			ProjectingFeatureStep bSrc = (ProjectingFeatureStep) bpart.getStep(null);
			bSelect = (SelectStatement) bSrc.getPlannedStatement();
			vectors.add(bSrc.getDistributionVector());
		}

		if (aModel != null) {
			RedistFeatureStep tempTab =
					redist(apart,aModel,targetGroup);
			apart.setStep(tempTab);
			aSelect = tempTab.buildNewSelect(getPlannerContext());
			vectors.add(tempTab.getTargetTempTable().getDistributionVector(getSchemaContext()));
		} else {
			ProjectingFeatureStep aSrc = (ProjectingFeatureStep) apart.getStep(null);
			aSelect = (SelectStatement) aSrc.getPlannedStatement();
			vectors.add(aSrc.getDistributionVector());
		}
		
		
		components.add(apart);
		components.add(bpart);
		
		SelectStatement finalJoinKern = DMLStatementUtils.compose(getSchemaContext(), aSelect, bSelect);
		
		DMLExplainRecord boundExplain = DMLExplainReason.CARTESIAN_JOIN.makeRecord();
		if (combinedCost.getRowCount() > -1)
			boundExplain = boundExplain.withRowEstimate(combinedCost.getRowCount());

		JoinedPartitionEntry sipe = 
				new JoinedPartitionEntry(getPlannerContext(),
						getSchemaContext(), 
						getBasis(),
						finalJoinKern,
						components,
						targetGroup, 
						vectors,
						combinedCost,
						getParentTransform(),
						getFeaturePlanner(),
						isOuterJoin(),
						false,
						boundExplain);

		return sipe;		

	}
	
	private RedistFeatureStep redist(PartitionEntry origEntry, Model model, PEStorageGroup targetGroup) throws PEException {
		List<Integer> redistOnColumns = null;
		if (model == Model.BROADCAST)
			redistOnColumns = Collections.emptyList();
		else {
			// choose the least broad distribution of the current entry
			Set<DistributionVector> vectors = origEntry.getDistributedOn();
			DistributionVector maximal = DistributionVector.findMaximal(vectors);
			if (maximal == null) 
				redistOnColumns = Collections.emptyList();
			else {
				List<RewriteKey> ascols = Functional.apply(
						origEntry.mapDistributionVectorColumns(new ArrayList<PEColumn>(maximal.getColumns(getSchemaContext()))),
						new UnaryFunction<RewriteKey,ColumnInstance>() {

							@Override
							public RewriteKey evaluate(ColumnInstance object) {
								return object.getRewriteKey();
							}
					
				});
				redistOnColumns = origEntry.getDistVectOffsets(ascols,origEntry.getTempTableSource());
			}
		}
		
		ProjectingFeatureStep src = (ProjectingFeatureStep) origEntry.getStep(null);
		
		RedistFeatureStep tempTab =
				src.redist(getPlannerContext(),
						getFeaturePlanner(),
						new TempTableCreateOptions(model,targetGroup)
							.distributeOn(redistOnColumns),
						null,
						DMLExplainReason.CARTESIAN_JOIN.makeRecord());
		
		return tempTab;
		
	}
	
	
	@Override
	protected boolean preferNewHead(ListSet<JoinedPartitionEntry> head) {
		return true;
	}

	
}
