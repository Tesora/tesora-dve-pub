// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.join;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.common.PEConstants;
import com.tesora.dve.db.Emitter.EmitOptions;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.ColumnKey;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.jg.DPart;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.Traversal;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.node.structural.JoinedTable;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.AliasInformation;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.CopyContext;
import com.tesora.dve.sql.transform.SchemaMapper;
import com.tesora.dve.sql.transform.execution.DMLExplainReason;
import com.tesora.dve.sql.transform.strategy.ExecutionCost;
import com.tesora.dve.sql.transform.strategy.featureplan.ProjectingFeatureStep;
import com.tesora.dve.sql.transform.strategy.join.JoinRewriteTransformFactory.JoinRewriteAdaptedTransform;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.sql.util.UnaryFunction;

/*
 * An original partition entry represents one of the partitions in the collapsed join graph.
 */
class OriginalPartitionEntry extends PartitionEntry {
	
	// the partition this entry is for
	protected DPart partition;
	// the projection, as buffer entries
	protected ListSet<BufferEntry> projectionEntries;
	// the where clause, also as buffer entries
	protected ListSet<BufferEntry> whereClause;
	// explicit joins as join entries
	protected ListSet<JoinBufferEntry> explicitJoins;
	// propagated restrictions
	protected ListOfPairs<ColumnKey,ExpressionNode> propagatedRestrictions;
	protected ListOfPairs<ColumnKey,ExpressionNode> restrictions;
	
	protected SelectStatement childCopy = null;

	protected JoinRewriteAdaptedTransform parentTransform = null;
	
	public OriginalPartitionEntry(SchemaContext sc, SelectStatement orig, DPart p) {
		super(sc, orig, DMLExplainReason.BASIC_PARTITION_QUERY.makeRecord());
		partition = p;
	}

	public void adapt(SelectStatement basis, 
			ListSet<BufferEntry> proj, 
			ListSet<BufferEntry> wc, 
			ListSet<JoinBufferEntry> joins,
			ListOfPairs<ColumnKey,ExpressionNode> propagated,
			ListOfPairs<ColumnKey,ExpressionNode> nonPropagated) throws PEException {
		projectionEntries = proj;
		whereClause = wc;
		explicitJoins = joins;
		propagatedRestrictions = propagated;
		if (propagatedRestrictions == null)
			propagatedRestrictions = new ListOfPairs<ColumnKey,ExpressionNode>();
		this.restrictions = nonPropagated;
		if (this.restrictions == null)
			this.restrictions = new ListOfPairs<ColumnKey,ExpressionNode>();
		buildChildCopy(basis);
		sane("adapt",projectionEntries.size(),childCopy.getProjectionEdge().size(),childCopy);
	}
	
	@Override
	protected String describe() {
		return "PE for {" + 
				Functional.join(partition.getTables(), ",", new UnaryFunction<String,TableKey>() {

					@Override
					public String evaluate(TableKey object) {
						return object.describe(getSchemaContext());
					}
					
				}) + "}";
	}
	
	public DPart getPartition() {
		return partition;
	}
	
	public ListSet<JoinBufferEntry> getExplicitJoins() {
		return explicitJoins;
	}
	
	public void setParentTransform(JoinRewriteAdaptedTransform parent) {
		parentTransform = parent;
	}
	
	public void buildChildCopy(SelectStatement basis) throws PEException {
		CopyContext cc = new CopyContext("buildChildCopy");
		ArrayList<ExpressionNode> newProj = new ArrayList<ExpressionNode>();
		for(BufferEntry be : projectionEntries) {
			newProj.add((ExpressionNode) be.getTarget().copy(cc));
		}
		UniquedExpressionList wcparts = new UniquedExpressionList();
		for(BufferEntry be : whereClause) {
			wcparts.add((ExpressionNode)be.getTarget().copy(cc));
		}
		MultiMap<TableKey,ExpressionNode> mapped = new MultiMap<TableKey,ExpressionNode>();
		for(Pair<ColumnKey, ExpressionNode> p : propagatedRestrictions) {
			mapped.put(p.getFirst().getTableKey(),ColumnReplacementTraversal.replaceColumn((ExpressionNode)p.getSecond().copy(cc), p.getFirst()));
		}
		for(Pair<ColumnKey,ExpressionNode> p : restrictions) {
			mapped.put(p.getFirst().getTableKey(), p.getSecond());
		}
		ListSet<TableKey> used = new ListSet<TableKey>();
		ListSet<FromTableReference> nftrs = new ListSet<FromTableReference>();
		for(JoinBufferEntry jbe : explicitJoins) {
			ListSet<TableKey> tablesInJoin = jbe.getTableKeys();
			FromTableReference hanger = null;
			for(FromTableReference ftr : nftrs) {
				TableInstance bt = ftr.getBaseTable();
				if (tablesInJoin.contains(bt.getTableKey())) {
					hanger = ftr;
					wcparts.addAll(mapped.get(bt.getTableKey()));
					break;
				}
				// not the base table, walk down the existing joins to find any pertinent tables
				for(JoinedTable jt : ftr.getTableJoins()) {
					if (tablesInJoin.contains(jt.getJoinedToTable().getTableKey())) {
						hanger = ftr;
						Collection<ExpressionNode> anyRestrictions = mapped.get(jt.getJoinedToTable().getTableKey());
						if (anyRestrictions != null && !anyRestrictions.isEmpty()) {
							ExpressionNode joinEx = jt.getJoinOn();
							UniquedExpressionList exprs = new UniquedExpressionList().addAll(ExpressionUtils.decomposeAndClause(joinEx))
									.addAll(anyRestrictions);
							jt.getJoinOnEdge().set(ExpressionUtils.safeBuildAnd(exprs.getExprs()));
						}
						break;
					}
				}
			}
			if (hanger == null) {
				TableKey ok = null;
				if (tablesInJoin.get(0).equals(jbe.getJoinedTable().getJoinedToTable().getTableKey()))
					ok = tablesInJoin.get(1);
				else
					ok = tablesInJoin.get(0);
				used.add(ok);
				hanger = new FromTableReference(ok.toInstance());
				nftrs.add(hanger);
				wcparts.addAll(mapped.get(ok));
			}
			ExpressionNode oldjex = jbe.getTarget();
			ExpressionNode njex = (oldjex != null ? (ExpressionNode)oldjex.copy(cc) : null);
			ExpressionNode nt = (ExpressionNode)jbe.getJoinedTable().getJoinedToTable().copy(cc);
			JoinedTable njt = new JoinedTable(nt,njex,jbe.getJoinedTable().getJoinType());
			hanger.addJoinedTable(njt);
			used.add(jbe.getJoinedTable().getJoinedToTable().getTableKey());
		}		
		ListSet<TableKey> allTables = new ListSet<TableKey>(partition.getTables());
		allTables.removeAll(used);
		// add one ftr for each unused table
		for(TableKey tk : allTables) {
			nftrs.add(new FromTableReference(tk.toInstance()));
			wcparts.addAll(mapped.get(tk));
		}

		ExpressionNode newWC = null;
		List<ExpressionNode> wcexprs = wcparts.getExprs();
		if (wcexprs.isEmpty())
			newWC = null;
		else if (wcexprs.size() == 1)
			newWC = wcexprs.get(0);
		else
			newWC = ExpressionUtils.buildAnd(wcexprs);
		
		childCopy = new SelectStatement(new AliasInformation())
				.setProjection(newProj)
				.setTables(nftrs)
				.setWhereClause(newWC);
		childCopy.getDerivedInfo().copyTake(basis.getDerivedInfo());
		childCopy.getDerivedInfo().clearLocalTables();
		childCopy.getDerivedInfo().addLocalTables(partition.getTables());
		
		SchemaMapper mapper = new SchemaMapper(Collections.singleton((DMLStatement)basis), childCopy, cc);
		childCopy.setMapper(mapper);
		if (childCopy.getProjectionEdge().size() != projectionEntries.size())
			throw new SchemaException(Pass.REWRITER, "Internal error in partition entry");
		childCopy.normalize(sc);
	}

	@Override
	public SelectStatement getJoinQuery(RewriteBuffers projInfo) throws PEException {
		return (SelectStatement) step.getPlannedStatement();
	}

	@Override
	public SelectStatement getCurrentExecQuery() {
		return (SelectStatement) step.getPlannedStatement();
	}
	
	public SelectStatement getChildCopy() {
		return childCopy;
	}
	
	@Override
	protected ExecutionCost computeScore() throws PEException {
		return ((ProjectingFeatureStep)getStep(null)).getCost();
	}

	@Override
	protected Set<TableKey> computeTables() {
		return partition.getTables();
	}

	@Override
	protected Set<DPart> computePartitions() {
		return Collections.singleton(partition);
	}

	@Override
	protected PEStorageGroup computeSourceGroup() {
		return partition.getStorageGroup(sc);
	}

	@Override
	protected Set<DistributionVector> computeDistributedOn() {
		HashSet<DistributionVector> dvs = new HashSet<DistributionVector>();
		for(TableKey ti : partition.getTables()) {
			DistributionVector dv = ti.getAbstractTable().getDistributionVector(sc);
			if (dv.usesColumns(sc))
				dvs.add(dv);
		}
		return dvs;
	}
	
	@Override
	public List<ExpressionNode> mapDistributedOn(List<ExpressionNode> in) {
		return in;
	}

	@Override
	public SelectStatement getTempTableSource() throws PEException {
		return getChildCopy();
	}

	@Override
	public List<BufferEntry> getBufferEntries() {
		return projectionEntries;
	}

	public String getPrePlanningState() {
		StringBuilder buf = new StringBuilder();
		buf.append(this);
		buf.append(PEConstants.LINE_SEPARATOR);
		buf.append(childCopy.getSQL(sc,EmitOptions.NONE.addMultilinePretty("  "), true)).append(PEConstants.LINE_SEPARATOR);
		return buf.toString();
	}

	public String getPostPlanningState() throws PEException {
		StringBuilder buf = new StringBuilder();
		buf.append(this).append(" ").append(getScore()).append(PEConstants.LINE_SEPARATOR);
		return buf.toString();
	}
	
	private static class ColumnReplacementTraversal extends Traversal {

		public static ExpressionNode replaceColumn(ExpressionNode en, ColumnKey newCol) {
			new ColumnReplacementTraversal(newCol).traverse(en);
			return en;
		}
		
		private ColumnKey newColumn;
		
		private ColumnReplacementTraversal(ColumnKey myColumn) {
			super(Order.POSTORDER, ExecStyle.ONCE);
			newColumn = myColumn;
		}
		
		@Override
		public LanguageNode action(LanguageNode in) {
			if (EngineConstant.COLUMN.has(in))
				return newColumn.toInstance();
			return in;
		}
		
	}

	@Override
	public List<ExpressionNode> getProjection() throws PEException {
		return childCopy.getProjection();
	}
	
}