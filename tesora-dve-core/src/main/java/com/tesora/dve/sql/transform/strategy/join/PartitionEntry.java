// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.join;





import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.RewriteKey;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.jg.DPart;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.LiteralExpression;
import com.tesora.dve.sql.schema.DistributionVector;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.PEAbstractTable;
import com.tesora.dve.sql.schema.PEColumn;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.execution.DMLExplainRecord;
import com.tesora.dve.sql.transform.strategy.ExecutionCost;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.ProjectingFeatureStep;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.UnaryFunction;

abstract class PartitionEntry {

	protected ExecutionCost score = null;
	protected Set<TableKey> tables = null;
	protected Set<DPart> partitions = null;
	protected SelectStatement basis;
	protected PEStorageGroup sourceGroup = null;
	protected Set<DistributionVector> distributedOn = null;
	
	// not always a projecting step - could be a redist step (temporarily)
	protected FeatureStep step = null;
	
	// these are the joins that this entry
	protected ListSet<JoinEntry> referencingJoins = new ListSet<JoinEntry>();
	// and these are the entries which are built upon this entry - valid for everything except partition entries
	protected ListSet<PartitionEntry> referencingEntries = new ListSet<PartitionEntry>();
	
	// once this partition entry has actually been scheduled, this will point to the ipe that
	// contains it
	protected PartitionEntry scheduled;
	
	protected DMLExplainRecord explain;
	
	protected final SchemaContext sc;
	protected final long id;
	
	public PartitionEntry(SchemaContext sc, SelectStatement orig, DMLExplainRecord expl) {
		basis = orig;
		this.sc = sc;
		this.id = sc.getNextObjectID();
		this.explain = expl;
	}
	
	public SelectStatement getBasis() {
		return basis;
	}
	
	public SchemaContext getSchemaContext() {
		return sc;
	}
	
	public ExecutionCost getScore() throws PEException {
		if (score == null) {
			if (scheduled != null)
				score = scheduled.getScore();
			else
				score = computeScore();
		}
		return score;
	}
	
	protected void clearScore() {
		score = null;
		// when my score is cleared any joins that reference this should get a new score
		for(JoinEntry je : referencingJoins)
			je.clearScore();
	}
	
	protected void setScore(ExecutionCost given) {
		if (score != null && given == null) return;
		score = given;
	}
	
	protected abstract ExecutionCost computeScore() throws PEException;

	protected abstract Set<TableKey> computeTables();
	
	public Set<TableKey> getSpanningTables() {
		if (tables == null)
			tables = computeTables();
		return tables;
	}		
	
	public boolean containsTable(TableKey tk) {
		return getSpanningTables().contains(tk);
	}
	
	protected abstract Set<DPart> computePartitions();
	
	public Set<DPart> getPartitions() {
		if (partitions == null)
			partitions = computePartitions();
		return partitions;
	}
	
	protected abstract PEStorageGroup computeSourceGroup();
	
	public PEStorageGroup getSourceGroup() {
		if (sourceGroup == null)
			sourceGroup = computeSourceGroup();
		return sourceGroup;
	}
	
	protected abstract Set<DistributionVector> computeDistributedOn();
	
	public Set<DistributionVector> getDistributedOn() {
		if (distributedOn == null)
			distributedOn = computeDistributedOn();
		return distributedOn;
	}
	
	public abstract List<ExpressionNode> getProjection() throws PEException;
	
	public abstract SelectStatement getJoinQuery(RewriteBuffers buffers) throws PEException;
		
	public abstract List<ExpressionNode> mapDistributedOn(List<ExpressionNode> in) throws PEException;

	// debugging help
	public abstract SelectStatement getCurrentExecQuery();
	
	public List<ColumnInstance> mapDistributionVectorColumns(List<PEColumn> in)
			throws PEException {
		ArrayList<ColumnInstance> out = new ArrayList<ColumnInstance>();
		if (in.isEmpty()) return out;
		PEAbstractTable<?> ofTable = in.get(0).getTable();
		TableKey tk = null;
		for(TableKey i : getSpanningTables()) {
			if (i.getTable().equals(ofTable)) {
				tk = i;
				break;
			}
		}
		for(PEColumn c : in) {
			out.add(new ColumnInstance(c,tk.toInstance()));
		}
		return out;
	}
		
	public List<Integer> mapDistributedOn(List<ExpressionNode> in, SelectStatement projSource) throws PEException {
		List<RewriteKey> ascols = Functional.apply(in, new UnaryFunction<RewriteKey,ExpressionNode>() {

			@Override
			public RewriteKey evaluate(ExpressionNode object) {
				return object.getRewriteKey();
			}
			
		});
		return getDistVectOffsets(ascols, projSource);
	}
	
	public List<Integer> getDistVectOffsets(List<RewriteKey> distVectColumns, SelectStatement ss)
			throws PEException {
		ArrayList<Integer> out = new ArrayList<Integer>();
		if (distVectColumns.isEmpty()) return out;
		HashMap<RewriteKey,Integer> dvectoff = new HashMap<RewriteKey,Integer>();
		for(int i = 0; i < distVectColumns.size(); i++)
			dvectoff.put(distVectColumns.get(i), i);
		TreeMap<Integer, Integer> results = new TreeMap<Integer,Integer>();
		for(int i = 0; i < ss.getProjectionEdge().size(); i++) {
			ExpressionNode en = ExpressionUtils.getTarget(ss.getProjectionEdge().get(i));
			RewriteKey rk = en.getRewriteKey();
			Integer any = dvectoff.get(rk);
			if (any != null)
				results.put(any,i);
		}
		return Functional.toList(results.values());
	}

	
	public abstract SelectStatement getTempTableSource() throws PEException;
	
	public abstract List<BufferEntry> getBufferEntries();
	
	public List<Integer> getInvisibleColumns() {
		ArrayList<Integer> out = new ArrayList<Integer>();
		List<BufferEntry> entries = getBufferEntries();
		for(int i = 0; i < entries.size(); i++) {
			if (entries.get(i).isInvisible())
				out.add(i);
		}
		return out;
	}
	
	protected abstract String describe() throws PEException;
	
	@Override
	public final String toString() {
		return getClass().getSimpleName() + "@" + id;
	}
	
	
	public void maybeForceDoublePrecision(ProjectingFeatureStep pfs) {
		List<BufferEntry> bufferEntries = getBufferEntries();
		SelectStatement in = (SelectStatement) pfs.getPlannedStatement();
		for(int i = 0; i < bufferEntries.size(); i++) {
			BufferEntry be = bufferEntries.get(i);
			if (!be.isInvisible()) continue;
			ExpressionNode t = ExpressionUtils.getTarget(in.getProjectionEdge().get(i));
			Edge<?, ExpressionNode> parentEdge = t.getParentEdge();
			FunctionCall fc = new FunctionCall(FunctionName.makeMult(), t, LiteralExpression.makeDoubleLiteral(1.0D));
			parentEdge.set(fc);
		}		
	}
	
	protected void sane(String reason, int nbuffers, int nproj, SelectStatement ss) {
		if (nbuffers != nproj) 
			throw new SchemaException(Pass.REWRITER,"Internal error at " + reason + "; found " + nbuffers + " but expected " + nproj + " on join kern " + ss.getSQL(sc));			
	}
	
	public void addReferencingJoin(JoinEntry je) {
		referencingJoins.add(je);
	}
	
	public void addReferencingEntry(PartitionEntry jre) {
		referencingEntries.add(jre);
	}

	public void setPlanned(PartitionEntry owner) {
		boolean notify = (scheduled != owner);
		scheduled = owner;
		if (notify)
			clearScore();
	}

	public PartitionEntry getActualEntry() {
		return this;
	}

	public void setExplain(DMLExplainRecord rec) {
		explain = rec;
	}
	
	public DMLExplainRecord getExplain() {
		return explain;
	}
	
	public void setStep(FeatureStep psf) {
		step = psf;
	}
	
	public FeatureStep getStep(RewriteBuffers projInfo) throws PEException {
		return step;
	}
	

}