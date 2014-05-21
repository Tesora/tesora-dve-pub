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
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.expression.ColumnKey;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.jg.DGJoin;
import com.tesora.dve.sql.jg.DPart;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.DistributionVector.Model;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEKey;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.TempTableCreateOptions;
import com.tesora.dve.sql.statement.dml.DMLStatementUtils;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.CopyVisitor;
import com.tesora.dve.sql.transform.execution.DMLExplainReason;
import com.tesora.dve.sql.transform.execution.DMLExplainRecord;
import com.tesora.dve.sql.transform.strategy.ExecutionCost;
import com.tesora.dve.sql.transform.strategy.TempGroupManager;
import com.tesora.dve.sql.transform.strategy.featureplan.ProjectingFeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.RedistFeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.RedistributionFlags;
import com.tesora.dve.sql.transform.strategy.join.JoinRewriteTransformFactory.JoinRewriteAdaptedTransform;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.sql.util.UnaryFunction;

/*
 * A clustered join entry is one where two or more tables all join to a single base table.
 * All the nonbase tables join to the same columns on the base table.
 */
public class ClusteredJoinEntry extends JoinEntry {

	protected List<RegularJoinEntry> parts;
	protected ListSet<RegularJoinEntry> informal;
	protected OriginalPartitionEntry baseEntry;
	List<ColumnKey> baseJoinColumns;
	ExecutionCost originalScore;
	
	ListSet<DGJoin> joins;
	DGJoin representativeJoin;
	PEStorageGroup nonBaseGroup;

	public ClusteredJoinEntry(SchemaContext sc, SelectStatement basis, 
			JoinRewriteAdaptedTransform parent, JoinRewriteTransformFactory factory,
			OriginalPartitionEntry base, List<RegularJoinEntry> ordered, ListSet<RegularJoinEntry> informal,
			PEStorageGroup nonBaseGroup,
			List<ColumnKey> joinColumns) throws PEException {
		super(parent.getPlannerContext(),sc, basis,parent, factory);
		parts = ordered;
		this.informal = informal;
		baseEntry = base;
		for(RegularJoinEntry rje : ordered)
			rje.addReference(this);
		for(RegularJoinEntry rje : informal)
			rje.addReference(this);
		this.nonBaseGroup = nonBaseGroup;
		this.baseJoinColumns = joinColumns;
		joins = new ListSet<DGJoin>();
		DGJoin loj = null;
		DGJoin roj = null;
		for(RegularJoinEntry rje : parts) {
			joins.add(rje.getJoin());
			if (rje.isOuterJoin()) {
				if (rje.getJoin().getJoin().getJoinType().isLeftOuterJoin()) {
					loj = rje.getJoin();
				} else if (rje.getJoin().getJoin().getJoinType().isRightOuterJoin()) {
					if (roj == null) roj = rje.getJoin();
				}
			}
		}
		if (loj != null)
			representativeJoin = loj;
		else if (roj != null)
			representativeJoin = roj;
		else if (!parts.isEmpty())
			representativeJoin = parts.get(0).getJoin();
		else
			representativeJoin = informal.get(0).getJoin();
		originalScore = estimateScore();
	}

	public List<RegularJoinEntry> getComponentParts() {
		return parts;
	}
	
	@Override
	public DGJoin getJoin() {
		return representativeJoin;
	}
	
	@Override
	public ListSet<DGJoin> getClusteredJoins() {
		return joins;
	}

	@Override
	public ListSet<DPart> getPartitions() {
		ListSet<DPart> out = new ListSet<DPart>();
		for(RegularJoinEntry rje : parts)
			out.addAll(rje.getPartitions());
		for(RegularJoinEntry rje : informal)
			out.addAll(rje.getPartitions());
		return out;
	}

	@Override
	public JoinedPartitionEntry schedule(List<JoinedPartitionEntry> ipes) throws PEException {
		throw new PEException("unsupported: scheduling an intint clustered join");
	}

	@Override
	public String toString() {
		return super.toString() + " clustered join on " + Functional.joinToString(parts, ", ");
	}

	@Override
	protected boolean preferNewHead(ListSet<JoinedPartitionEntry> head) {
		return true;
	}

	protected ExecutionCost estimateScore() throws PEException {
		ExecutionCost baseScore = baseEntry.getScore();
		ExecutionCost maximalScore = baseScore;
		for(RegularJoinEntry rje : parts) {
			DGJoin dgj = rje.getJoin();
			TableKey otherTable = null;
			if (baseEntry.containsTable(dgj.getLeftTables().get(0))) {
				otherTable = dgj.getRightTable(); 
			} else {
				otherTable = dgj.getRightTable();
			}
			List<ExpressionNode> cols = dgj.getRedistJoinExpressions(otherTable);
			ListSet<PEColumn> columns = new ListSet<PEColumn>();
			for(ExpressionNode en : cols) {
				if (en instanceof ColumnInstance) {
					ColumnInstance ci = (ColumnInstance) en;
					columns.add(ci.getPEColumn());
				}
			}
			boolean haveUnique = false;
			for(PEKey pek : otherTable.getAbstractTable().getKeys(getSchemaContext())) {
				if (pek.getColumns(getSchemaContext()).size() == columns.size() && 
						columns.containsAll(pek.getColumns(getSchemaContext())))
					if (pek.isUnique()) {
						haveUnique = true;
						break;
					}
			}
			PartitionEntry otherEntry = rje.getLeftPartition() == baseEntry ? rje.getRightPartition() : rje.getLeftPartition();
			ExecutionCost myscore = null;
			if (!haveUnique) {
				myscore = otherEntry.getScore();
			} else {
				List<ExecutionCost> scores = Arrays.asList(otherEntry.getScore(),baseScore);
				myscore = ExecutionCost.minimize(scores);
			}
			maximalScore = ExecutionCost.maximize(Arrays.asList(new ExecutionCost[] { maximalScore, myscore }));
		}
		return maximalScore;
	}
	
	@Override
	protected ExecutionCost computeScore() {
		// depending on the kinds of joins - this is either the score of the most restrictive
		// partition entry or else the score of the base table.
		return originalScore;
	}

	@Override
	public JoinedPartitionEntry schedule()
			throws PEException {
		return build(baseEntry);
	}

	private JoinedPartitionEntry build(PartitionEntry base) throws PEException {
		int groupSize = -1;
		ExecutionCost myScore = getScore();
		if (myScore.getConstraint() != null)
			if (myScore.getConstraint().getType().isUnique())
				groupSize = 1;
		if (groupSize == -1)
			groupSize = myScore.getGroupScore();
		TempGroupManager tgm = getPlannerContext().getTempGroupManager();
		PEStorageGroup tempGroup = (groupSize == 1 ? tgm.getGroup(true) : tgm.getLatestGroup(groupSize));
		
		// so, we're first going to redist the base partition on the dist vect to the temp group
		// the order of the baseJoinColumns is going to have to be preserved - remember that
		List<ExpressionNode> redistOn = Functional.apply(baseJoinColumns, new UnaryFunction<ExpressionNode, ColumnKey>() {

			@Override
			public ColumnInstance evaluate(ColumnKey object) {
				return object.toInstance();
			}
			
		});
		List<ExpressionNode> mro = base.mapDistributedOn(redistOn);
		List<Integer> mappedRedistOn = base.mapDistributedOn(mro, base.getTempTableSource());
		
		RedistFeatureStep redisted =
				((ProjectingFeatureStep)base.getStep(null)).redist(
						getPlannerContext(),
						getFeaturePlanner(),
						new TempTableCreateOptions(Model.STATIC, tempGroup)
						.withInvisibleColumns(base.getInvisibleColumns())
						.withRowCount(baseEntry.getScore().getRowCount())
						.distributeOn(mappedRedistOn),
						new RedistributionFlags(),
						DMLExplainReason.CLUSTERED_JOIN.makeRecord());

		// so the base has been moved to the temp group
		// now we have to build the lookup table
		RedistFeatureStep lookupTable =
				LookupTableJoinStrategy.buildLookupTableRedist(
						getPlannerContext(),
						baseEntry,
						redisted,
						this,
						nonBaseGroup,
						true);

		// now, for each of the partitions, build one lookup table join step
		
		LinkedHashMap<RegularJoinEntry, Pair<RedistFeatureStep,OriginalPartitionEntry>> fetches =
				new LinkedHashMap<RegularJoinEntry,Pair<RedistFeatureStep,OriginalPartitionEntry>>();
		
		for(RegularJoinEntry rje : parts) {
			// the lookup table join step is between other and lookupTable
			buildLookupJoinStep(rje,
					lookupTable,
					tempGroup,
					base, 
					fetches);

		}
		
		for(RegularJoinEntry rje : informal) {
			buildLookupJoinStep(rje,lookupTable,tempGroup,base, fetches);
		}
		
		

		List<RegularJoinEntry> overallOrder = new ArrayList<RegularJoinEntry>();
		overallOrder.addAll(parts);
		overallOrder.addAll(informal);

		List<SelectStatement> tts = new ArrayList<SelectStatement>();
		HashSet<DistributionVector> dists = new HashSet<DistributionVector>();
		List<PartitionEntry> origOrdered = new ArrayList<PartitionEntry>();
		
		SelectStatement baseSelect = (SelectStatement) CopyVisitor.copy(redisted.buildNewSelect(getPlannerContext()));
		// add base first
		baseSelect = LookupTableJoinStrategy.filterEntryProjection(baseSelect, base);
		tts.add(baseSelect);
		origOrdered.add(base);
		for(RegularJoinEntry rje : overallOrder) {
			Pair<RedistFeatureStep,OriginalPartitionEntry> p = fetches.get(rje);
			RedistFeatureStep rfs = p.getFirst();
			SelectStatement lhs = rfs.buildNewSelect(getPlannerContext());
			lhs = LookupTableJoinStrategy.filterEntryProjection(lhs,p.getSecond());
			tts.add(lhs);		
			origOrdered.add(p.getSecond());
			dists.add(rfs.getTargetTempTable().getDistributionVector(getSchemaContext()));
			p.getSecond().setStep(rfs);
		}
		
		SelectStatement finalJoinKern = DMLStatementUtils.composeMulti(getSchemaContext(), tts);
		
		DMLExplainRecord record = DMLExplainReason.CLUSTERED_JOIN.makeRecord();
		if (getScore().getRowCount() > -1) {
			record = record.withRowEstimate(getScore().getRowCount());
		}

		JoinedPartitionEntry ipe =
				new JoinedPartitionEntry(getPlannerContext(),
						getSchemaContext(),
						getBasis(),
						finalJoinKern,
						origOrdered,
						tempGroup,
						dists,
						getScore(),
						getParentTransform(),
						getFeaturePlanner(),
						isOuterJoin(),
						false,
						record);
		
		ipe.setScore(getScore());
		return ipe;
	}
	
	@Override
	public JoinedPartitionEntry schedule(JoinedPartitionEntry head) throws PEException {
		return build(head);
	}

	private void buildLookupJoinStep(
			RegularJoinEntry rje,
			RedistFeatureStep lookupTable,
			PEStorageGroup tempGroup,
			PartitionEntry base,
			Map<RegularJoinEntry, Pair<RedistFeatureStep,OriginalPartitionEntry>> fetches) throws PEException {
		OriginalPartitionEntry other = rje.getOtherPartition(baseEntry);
		DGJoin dgj = rje.getJoin();
		TableKey oti = null;
		TableKey baseTable = null;
		if (baseEntry.containsTable(dgj.getLeftTable())) {
			oti = dgj.getRightTable(); 
			baseTable = dgj.getLeftTable();
		} else {
			oti = dgj.getLeftTable();
			baseTable = dgj.getRightTable();
		}
		List<ExpressionNode> odvcols = other.mapDistributedOn(dgj.getRedistJoinExpressions(oti));
		List<ExpressionNode> dvcols = base.mapDistributedOn(dgj.getRedistJoinExpressions(baseTable));
		RedistFeatureStep out =
				LookupTableJoinStrategy.buildLookupJoinRedist(
						getPlannerContext(),
						lookupTable,
						dvcols,
						tempGroup,
						rje,
						odvcols,
						other);
		fetches.put(rje,new Pair<RedistFeatureStep,OriginalPartitionEntry>(out,other));
	}
	
}
