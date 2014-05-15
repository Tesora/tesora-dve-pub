// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.joinsimplification;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.expression.ColumnKey;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.node.structural.JoinedTable;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.Pair;

public class MultijoinSimplifier extends Simplifier {

	@Override
	public boolean applies(SchemaContext sc, DMLStatement dmls) throws PEException {
		return EngineConstant.FROMCLAUSE.has(dmls);
	}

	@Override
	public DMLStatement simplify(SchemaContext sc,DMLStatement in, JoinSimplificationTransformFactory parent) throws PEException {
		List<LanguageNode> explicitJoins = EngineConstant.FROMCLAUSE.getMulti(in);
		
		String before = (parent.emitting() ? in.getSQL(sc) : null);
		
		// suppose we have something of the form
		// A a inner join B b on a.id=b.id (any kind) join C c on b.sid=c.sid and c.id=a.id
		// we seek to turn the nway join at the end (C is joined to both A & B) into a regular binary join by rewriting
		// a.id to be b.id (which is legal because of the inner join).  This rewrite always has to be applied left to right.
		boolean any = false;
		for(LanguageNode ln : explicitJoins) {
			if (ln instanceof FromTableReference) {
				FromTableReference ftr = (FromTableReference) ln;
				if (processTableChain(ftr, sc)) {
					ftr.getBlock().clear();
					any = true;
				}
			}
		}
		if (any) {
			if (parent.emitting()) {
				parent.emit(" MJS in:  " + before);
				parent.emit(" MJS out: " + in.getSQL(sc));
			}
			
			return in;
		}
		return null;
	}

	private static void accTable(Map<TableKey,Integer> into, TableKey tk) {
		Integer already = into.get(tk);
		if (already == null) 
			into.put(tk, new Integer(1));
		else
			into.put(tk, new Integer(already.intValue() + 1));
	}
	
	private static void decTable(Map<TableKey,Integer> into, TableKey tk) {
		Integer already = into.get(tk);
		if (already == null) return;
		if (already.intValue() == 1) {
			into.remove(tk);
		} else {
			into.put(tk, new Integer(already.intValue() - 1));
		}
	}
	
	private static boolean processTableChain(FromTableReference ftr, final SchemaContext sc) {
		MultiMap<ColumnKey,ColumnKey> equivs = new MultiMap<ColumnKey,ColumnKey>();
		boolean any = false;
		for(JoinedTable jt : ftr.getTableJoins()) {
			HashMap<TableKey,Integer> tabs = new HashMap<TableKey,Integer>();
			ListOfPairs<ColumnInstance,ColumnInstance> equijoins = new ListOfPairs<ColumnInstance,ColumnInstance>();
			ExpressionNode joinEx = jt.getJoinOn();
			List<ExpressionNode> decompAnd = ExpressionUtils.decomposeAndClause(joinEx);
			for(ExpressionNode en : decompAnd) {
				if (Boolean.TRUE.equals(EngineConstant.EQUIJOIN.getValue(en, sc))) {
					FunctionCall fc = (FunctionCall) en;
					ExpressionNode lhn = fc.getParametersEdge().get(0);
					ExpressionNode rhn = fc.getParametersEdge().get(1);
					if (lhn instanceof ColumnInstance && rhn instanceof ColumnInstance) {
						ColumnInstance lhs = (ColumnInstance) lhn;
						ColumnInstance rhs = (ColumnInstance) rhn;
						equijoins.add(lhs, rhs);
						accTable(tabs, lhs.getTableInstance().getTableKey());
						accTable(tabs, rhs.getTableInstance().getTableKey());
					}
				}				
			}
			if (jt.getJoinType().isInnerJoin()) {
				for(Pair<ColumnInstance,ColumnInstance> p : equijoins) {
					ColumnKey lk = p.getFirst().getColumnKey();
					ColumnKey rk = p.getSecond().getColumnKey();
					equivs.put(lk, rk);
					equivs.put(rk, lk);
				}
			}
			if (tabs.size() > 2) {
				TableInstance target = jt.getJoinedToTable();
				TableKey targetTK = target.getTableKey();
				tabs.remove(targetTK);
				// nway join.
				// we can't change the target, but we can change the nontarget side
				MultiMap<Pair<ColumnInstance,ColumnInstance>,ColumnKey> modCandidates = new MultiMap<Pair<ColumnInstance,ColumnInstance>,ColumnKey>();
				for(Pair<ColumnInstance,ColumnInstance> p : equijoins) {
					ColumnInstance toModify = p.getFirst().getTableInstance() == target ? p.getSecond() : p.getFirst();
					Collection<ColumnKey> eq = equivs.get(toModify.getColumnKey());
					if (eq == null || eq.isEmpty()) continue;
					// only select column keys whose tab keys are in the keyset
					List<ColumnKey> candidates = new ArrayList<ColumnKey>();
					for(ColumnKey ck : eq) {
						if (tabs.containsKey(ck.getTableKey())) {
							candidates.add(ck);
						}
					}
					if (candidates.isEmpty()) continue;
					for(ColumnKey ck : candidates) {
						modCandidates.put(p,ck);
					}
				}
				if (modCandidates.isEmpty())
					continue;
				// now for all the repls in modCandidates, choose the best table.  The best table is the one that is in all of the entries.
				LinkedHashSet<TableKey> intersection = null;
				for(Pair<ColumnInstance,ColumnInstance> p : modCandidates.keySet()) {
					LinkedHashSet<TableKey> ctks = new LinkedHashSet<TableKey>();
					for(ColumnKey ck : modCandidates.get(p))
						ctks.add(ck.getTableKey());
					if (intersection == null)
						intersection = ctks;
					else 
						intersection.retainAll(ctks);
				}
				if (intersection.isEmpty())
					continue;
				// we're just going to arbitrarily choose the first one
				TableKey candidateTable = intersection.iterator().next();
				// set up replacement values - but don't make the change yet
				HashMap<ColumnInstance,ColumnInstance> replacements = new HashMap<ColumnInstance,ColumnInstance>();
				for(Pair<ColumnInstance,ColumnInstance> p : modCandidates.keySet()) {
					LinkedHashSet<ColumnKey> replCandidates = new LinkedHashSet<ColumnKey>(modCandidates.get(p));
					for(Iterator<ColumnKey> riter = replCandidates.iterator(); riter.hasNext();) {
						if (riter.next().getTableKey().equals(candidateTable)) {
							// leave it alone
						} else {
							riter.remove();
						}
					}
					if (replCandidates.size() != 1) {
						continue;
					}
					ColumnKey replacementColumn = replCandidates.iterator().next();
					if (p.getFirst().getTableInstance() == target) {
						replacements.put(p.getSecond(),replacementColumn.toInstance());
						decTable(tabs,p.getSecond().getTableInstance().getTableKey());
					} else {
						replacements.put(p.getFirst(), replacementColumn.toInstance());
						decTable(tabs,p.getFirst().getTableInstance().getTableKey());
					}
				}
				if (tabs.size() == 1) {
					// success!  make the changes
					for(Map.Entry<ColumnInstance, ColumnInstance> me : replacements.entrySet()) {
						Edge<?,ExpressionNode> parentEdge = me.getKey().getParentEdge();
						parentEdge.set(me.getValue());
					}
					joinEx.getBlock().clear();
					any = true;
				}
			}
		}
		return any;
	}

}
