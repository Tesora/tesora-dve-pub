// OS_STATUS: public
package com.tesora.dve.sql.jg;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.Subquery;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.expression.TableJoin;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.node.structural.JoinedTable;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.MultiTableDMLStatement;
import com.tesora.dve.sql.transform.ColumnInstanceCollector;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.Pair;

public class UncollapsedJoinGraph extends JoinGraph {

	Map<FromTableReference,Integer> branches;
	
	public UncollapsedJoinGraph(SchemaContext sc, MultiTableDMLStatement s) {
		this(sc, s,false);
	}
	
	public UncollapsedJoinGraph(SchemaContext sc, MultiTableDMLStatement s, boolean ignoreSubqueries) {
		super(s);
		branches = new HashMap<FromTableReference,Integer>();
		build(sc, ignoreSubqueries);
	}
	
	protected void build(SchemaContext sc, boolean ignoreSubqueries) {
		if (!stmt.supportsPartitions())
			throw new SchemaException(Pass.PLANNER, "Attempt to compute partitions on a " + stmt.getClass().getSimpleName());
		HashMap<TableKey,DunPart> parts = new HashMap<TableKey,DunPart>();
		takeTables(sc, parts, ignoreSubqueries);
		takeWhereClause(sc, parts);
		
	}
	
	private void takeTables(SchemaContext sc, Map<TableKey,DunPart> parts, boolean ignoreSubqueries) {
		int branch = -1;
		for(FromTableReference ftr : stmt.getTables()) {
			branch++;
			branches.put(ftr,branch);
			boolean grouping = false;
			TableInstance base = ftr.getBaseTable();
			List<JoinedTable> allJoins = null;
			if (base != null)
				allJoins = ftr.getTableJoins();
			else {
				allJoins = new LinkedList<JoinedTable>();
				ExpressionNode targ = ftr.getTarget();
				boolean skip = false;
				while(base == null) {
					if (targ instanceof TableInstance) {
						base = (TableInstance) targ;
					} else if (targ instanceof TableJoin) {
						TableJoin tj = (TableJoin) targ;
						if (tj.isGrouped()) grouping = true;
						allJoins.addAll(0, tj.getJoins());
						targ = tj.getFactor();
					} else if (targ instanceof Subquery) {
						skip = true;
						break;
					}
				}
				if (skip) continue;
			}
			getPartition(base.getTableKey(),parts);	
			int ojoffset = -1;
			for(JoinedTable jt : allJoins) {
				TableInstance targ = jt.getJoinedToTable();
				if (targ == null) {
					if (ignoreSubqueries) continue;
					unhandled("join target not a table");
				}
				ojoffset++;
				getPartition(targ.getTableKey(),parts);
				buildEdges(sc,jt.getJoinOn(), jt, branch, ojoffset, grouping, parts);				
			}
		}
	}
	
	private void takeWhereClause(SchemaContext sc, Map<TableKey,DunPart> parts) {
		buildEdges(sc, stmt.getWhereClause(),null,-1,-1, false, parts);
	}
	
	private DunPart getPartition(TableKey tk, Map<TableKey,DunPart> parts) {
		DunPart dp = parts.get(tk);
		if (dp == null) {
			dp = new DunPart(tk,vertices.size());
			parts.put(tk,dp);
			vertices.add(dp);
		}
		return dp;
	}
	
	private ListSet<DunPart> getParts(ListSet<TableKey> forKeys, Map<TableKey,DunPart> parts) {
		ListSet<DunPart> out = new ListSet<DunPart>();
		for(TableKey tk : forKeys) {
			DunPart dp = parts.get(tk);
			if (dp == null) {
				dp = new DunPart(tk,vertices.size());
				parts.put(tk,dp);
				vertices.add(dp);
			}
			out.add(dp);
		}
		return out;
	}
	
	private void buildEdges(SchemaContext sc, 
			ExpressionNode joinEx, JoinedTable enclosing, 
			int branch, int offset, 
			boolean grouped, 
			Map<TableKey,DunPart> parts) {
		// if the join ex refers to more than two tables - multijoin
		List<FunctionCall> equijoins = findEquijoins(sc,joinEx, enclosing);
		LinkedHashMap<Pair<DunPart,DunPart>,List<FunctionCall>> byPartition = new LinkedHashMap<Pair<DunPart,DunPart>,List<FunctionCall>>();
		HashMap<FunctionCall,Pair<ListSet<TableKey>,ListSet<TableKey>>> colKeysByFunction =
				new HashMap<FunctionCall,Pair<ListSet<TableKey>,ListSet<TableKey>>>();
		for(FunctionCall fc : equijoins) {
			ListSet<TableKey> lcis = ColumnInstanceCollector.getTableKeys(fc.getParametersEdge().get(0));
			ListSet<TableKey> rcis = ColumnInstanceCollector.getTableKeys(fc.getParametersEdge().get(1));
			colKeysByFunction.put(fc, new Pair<ListSet<TableKey>,ListSet<TableKey>>(lcis,rcis));
			
			ListSet<DunPart> lparts = getParts(lcis,parts);
			ListSet<DunPart> rparts = getParts(rcis,parts);
			ListSet<DunPart> joinToParts = null;
			ListSet<DunPart> joinFromParts = null;
			if (lparts.size() == 1 && rparts.size() == 1) {
				if (lparts.get(0).getGraphID() < rparts.get(0).getGraphID()) {
					joinFromParts = lparts;
					joinToParts = rparts;
				} else {
					joinFromParts = rparts;
					joinToParts = lparts;
				}
			} else {
				joinToParts = rparts;
				if (joinToParts.size() > 1)
					joinToParts = lparts;
				if (joinToParts.size() > 1)
					throw new SchemaException(Pass.PLANNER,"Too complex join expression: '" + fc + "'");
				joinFromParts = (joinToParts == rparts ? lparts : rparts);
			}
			for(DunPart dp : joinFromParts) {
				Pair<DunPart,DunPart> partKey = new Pair<DunPart,DunPart>(dp,joinToParts.get(0));
				List<FunctionCall> any = byPartition.get(partKey);
				if (any == null) {
					any = new ArrayList<FunctionCall>();
					byPartition.put(partKey,any);
				}
				any.add(fc);
			}
		}
		DGJoin encjoinSpec = null;
		if (enclosing != null) {
			TableKey joinedToTable = enclosing.getJoinedToTable().getTableKey();
			ListSet<TableKey> tabs = ColumnInstanceCollector.getTableKeys(joinEx);
			joinedToTable = enclosing.getJoinedToTable().getTableKey();
			tabs.remove(joinedToTable);
			if (byPartition.keySet().size() > 1) {
				encjoinSpec = new DGMultiJoin(enclosing,branch,offset,grouped,tabs,joinedToTable);
			} else {
				encjoinSpec = new DGJoin(enclosing,branch,offset,grouped,tabs,joinedToTable);
			}
		}
		for(Pair<DunPart,DunPart> p : byPartition.keySet()) {
			DunPart joinedTo = null;
			DGJoin joinSpec = encjoinSpec;
			if (encjoinSpec == null) {
				joinSpec = new DGJoin(null,branch,offset,grouped,p.getFirst().getTables(),p.getSecond().getTables().get(0));
			} else if (!encjoinSpec.isInnerJoin()) {
				joinedTo = getPartition(enclosing.getJoinedToTable().getTableKey(),parts);
			}
			JoinEdge e = null;
			if (enclosing != null) {
				if (p.getFirst() == joinedTo)
					e = new JoinEdge(edges.size(),p.getSecond(), p.getFirst(), joinSpec);
				else
					e = new JoinEdge(edges.size(),p.getFirst(), p.getSecond(), joinSpec);
			} else {
				e = new JoinEdge(edges.size(),p.getFirst(),p.getSecond(),joinSpec);
			}
			for(FunctionCall fc : byPartition.get(p)) {
				Pair<ListSet<TableKey>,ListSet<TableKey>> cks = colKeysByFunction.get(fc);
				TableKey lhk = e.getLHSTab();
				TableKey rhk = e.getRHSTab();
				if (cks.getFirst().contains(lhk) && cks.getSecond().contains(rhk)) {
					e.addJoinExpression(fc.getParametersEdge().get(0),fc.getParametersEdge().get(1));
				} else if (cks.getFirst().contains(rhk) && cks.getSecond().contains(lhk)) {
					e.addJoinExpression(fc.getParametersEdge().get(1), fc.getParametersEdge().get(0));
				} else {
					throw new SchemaException(Pass.PLANNER, "Malformed join expression: " + fc);
				}
			} 
			if (!hasEdge(p.getFirst().getEdges(), e))
				p.getFirst().getEdges().add(e);
			if (!hasEdge(p.getSecond().getEdges(), e))
				p.getSecond().getEdges().add(e);
			if (!hasEdge(edges, e))
				edges.add(e);
		}
	}

	private static List<FunctionCall> findEquijoins(final SchemaContext sc, ExpressionNode en, JoinedTable enclosing) {
		if (en == null) return Collections.emptyList();
		return EngineConstant.EQUIJOINS.getValue(en,sc);
	}

	@Override
	public Map<FromTableReference, Integer> getBranches() {
		return branches;
	}
	
	private boolean hasEdge(List<JoinEdge> edges, JoinEdge newEdge) {
		boolean ret = false;
		
		for(JoinEdge edge : edges) {
			if (edge.isSame(newEdge)) {
				return true;
			}
		}
		
		return ret;
	}
}
