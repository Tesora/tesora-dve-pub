// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.join;

import java.util.List;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.jg.DPart;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.node.structural.JoinedTable;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.ColumnInstanceCollector;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.Pair;

class ExplicitJoinBuffer extends FinalBuffer {

	protected PartitionLookup partitionLookup;
	protected MultiMap<FromTableReference,JoinedTable> logicalOrder;
	
	public ExplicitJoinBuffer(Buffer prev, PartitionLookup pl) {
		super(BufferKind.J, prev,pl);
		partitionLookup = pl;
		logicalOrder = new MultiMap<FromTableReference,JoinedTable>();
	}
	
	@Override
	public void adapt(SchemaContext sc, SelectStatement stmt) throws PEException {
		P2ProjectionBuffer proj = (P2ProjectionBuffer) getBuffer(BufferKind.P2);
		for(FromTableReference ftr : stmt.getTables()) {
			if (ftr.getBaseSubquery() != null)
				throw new PEException("Failed to handle nested query correctly");
			Pair<ExpressionNode,List<JoinedTable>> unrolled = ftr.getUnrolledOrder();
			for(int i = 0; i < unrolled.getSecond().size(); i++) {
				JoinedTable jt = unrolled.getSecond().get(i);
				logicalOrder.put(ftr, jt);
				ExpressionNode joinEx = jt.getJoinOn();
				JoinBufferEntry jbe = null;
				ListSet<DPart> parts = null;
				ListSet<ExpressionNode> redistOn = null;
				ListSet<ColumnInstance> allColumns = null;
				if (joinEx != null) {
					partitionLookup.getRestrictionManager().take(joinEx);
					allColumns = ColumnInstanceCollector.getColumnInstances(joinEx);
					Pair<ListSet<ExpressionNode>, ListSet<ColumnInstance>> argh = buildRedistExprs(sc, joinEx);
					ListSet<ExpressionNode> exprs = new ListSet<ExpressionNode>();
					redistOn = argh.getFirst();
					exprs.addAll(redistOn);
					exprs.addAll(argh.getSecond());
					ListSet<TableKey> tabs = ColumnInstanceCollector.getTableKeys(allColumns);
					parts = partitionLookup.getPartitionsFor(tabs);
					jbe = new JoinBufferEntry(joinEx,ftr,jt, tabs, exprs);
				} else {
					// one of those joins with a regular where clause - in this case we're going to look
					// at whatever the lhs is.  
					ListSet<TableKey> tabs = new ListSet<TableKey>();
					tabs.add(ftr.getBaseTable().getTableKey());
					tabs.add(jt.getJoinedToTable().getTableKey());
					parts = partitionLookup.getPartitionsFor(tabs);
					jbe = new JoinBufferEntry(null,ftr,jt,tabs,null);
				}
				if (parts.size() > 1 && redistOn != null) {
					// bridging.  add one entry for each column used in the expression; then do the compound entries
					for(ColumnInstance ci : allColumns) {
						BufferEntry nbe = proj.addForJoin(ci,false);
						jbe.addDependency(nbe);
					}
					for(ExpressionNode en : redistOn) {
						if (en instanceof ColumnInstance) continue;
						ListSet<DPart> eparts = null;
						ListSet<ColumnInstance> cols = ColumnInstanceCollector.getColumnInstances(en); 
						ListSet<TableKey> tabs = ColumnInstanceCollector.getTableKeys(cols);
						eparts = partitionLookup.getPartitionsFor(tabs);
						BufferEntry nbe = proj.addForJoin(en, true);
						jbe.addDependency(nbe);
						jbe.registerCompoundRedist(en, (RedistBufferEntry) nbe);
						nbe.registerCompoundRedist(en, (RedistBufferEntry) nbe);
						proj.partitionInfo.add(nbe,eparts);
					}
				}
				partitionInfo.add(jbe, parts);
				add(jbe);
			}
		}
	}		
	
	public ListSet<JoinBufferEntry> getJoinEntriesFor(DPart p) {
		ListSet<JoinBufferEntry> out = new ListSet<JoinBufferEntry>();
		for(BufferEntry be : getEntries()) {
			JoinBufferEntry jbe = (JoinBufferEntry) be;
			if (partitionInfo.isOfPartition(jbe, p))
				out.add(jbe);
		}
		return out;
	}
	
	@Override
	public void setBridging(ListSet<BufferEntry> set) throws PEException {
		// no sense scoring
		bridging = set;
		scoredBridges = new ListSet<BufferEntry>(bridging);
	}
	
	public MultiMap<FromTableReference,JoinedTable> getLogicalOrder() {
		return logicalOrder;
	}
	
	private Pair<ListSet<ExpressionNode>, ListSet<ColumnInstance>> buildRedistExprs(SchemaContext sc, ExpressionNode joinEx) {
		List<ExpressionNode> decomp = ExpressionUtils.decomposeAndClause(joinEx);
		ListSet<ExpressionNode> out = new ListSet<ExpressionNode>();
		ListSet<ExpressionNode> nonEJ = new ListSet<ExpressionNode>();
		for(ExpressionNode en : decomp) {
			ListSet<FunctionCall> eqjs = EngineConstant.EQUIJOINS.getValue(en, sc);
			if (eqjs.isEmpty()) {
				nonEJ.add(en);
				continue;
			}
			for(FunctionCall fc : eqjs) {
				ListSet<TableKey> tabKeys = ColumnInstanceCollector.getTableKeys(fc);
				if (tabKeys.size() < 2) {
					// not a join
					nonEJ.add(fc);
					continue;
				} 
				out.addAll(fc.getParameters());
			}
		}
		return new Pair<ListSet<ExpressionNode>, ListSet<ColumnInstance>>(out,ColumnInstanceCollector.getColumnInstances(nonEJ));
	}
	
}