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

import java.io.PrintStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.sql.expression.ColumnKey;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.jg.CollapsedJoinGraph;
import com.tesora.dve.sql.jg.DPart;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.transform.ColumnInstanceCollector;
import com.tesora.dve.sql.transform.NullFunCollector;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.Pair;

public class RestrictionManager {

	private MultiMap<ColumnKey,ExpressionNode> restrictions;
	private MultiMap<ColumnKey,ColumnKey> restrictionForwarding;
	private ListSet<TableKey> nullTested;
	private boolean pruned;
	
	public RestrictionManager(CollapsedJoinGraph jg) {
		restrictions = new MultiMap<ColumnKey,ExpressionNode>();
		restrictionForwarding = jg.getRestrictionPropagationMap();
		nullTested = new ListSet<TableKey>();
		pruned = false;
	}
	
	public void takeNullTested(TableKey tk) {
		nullTested.add(tk);
	}
	
	public void take(ExpressionNode expr) {
		List<ExpressionNode> decomp = ExpressionUtils.decomposeAndClause(expr);
		for(ExpressionNode en : decomp) {
			// if the node is an is null - we can't use it
			ListSet<FunctionCall> nullFuns = NullFunCollector.collectNullFuns(en);
			if (!nullFuns.isEmpty()) {
				// has null funs - examine them
				for(FunctionCall fc : nullFuns) {
					ListSet<ColumnKey> ncks = ColumnInstanceCollector.getColumnKeys(fc);
					for(ColumnKey ick : ncks)
						nullTested.add(ick.getTableKey());
				}
				continue;
			}
			ListSet<ColumnKey> cks = ColumnInstanceCollector.getColumnKeys(en);
			if (cks.size() != 1) continue;
			restrictions.put(cks.get(0), en);
		}
	}
	
	public ListOfPairs<ColumnKey,ExpressionNode> findPropagatedRestrictions(DPart partition) {
		prune();
		// first find all columns that are in play by looking at the forwarding.
		ListSet<ColumnKey> columnsForPartition = new ListSet<ColumnKey>();
		ListSet<TableKey> tablesForPartition = partition.getTables();
		for(ColumnKey ck : restrictionForwarding.keySet()) {
			if (tablesForPartition.contains(ck.getTableKey()))
				columnsForPartition.add(ck);
		}
		if (columnsForPartition.isEmpty())
			return null;
		// this is a slice of the full multimap.  the keys are in partition; the values are elsewhere.
		LinkedHashMap<ColumnKey,Set<ColumnKey>> slice = new LinkedHashMap<ColumnKey,Set<ColumnKey>>();
		for(ColumnKey ck : columnsForPartition) {
			ListSet<ColumnKey> others = new ListSet<ColumnKey>();
			others.addAll(restrictionForwarding.get(ck));
			// now we have to go to those and all of theirs, unless they already exist or are in partition.
			boolean done = false;
			while(!done) {
				HashSet<ColumnKey> more = new HashSet<ColumnKey>();
				for(ColumnKey ick : others) {
					Set<ColumnKey> partners = (Set<ColumnKey>) restrictionForwarding.get(ick);
					for(ColumnKey ock : partners) {
						if (tablesForPartition.contains(ock.getTableKey()))
							continue;
						more.add(ock);
					}
				}
				done = !others.addAll(more);
			}
			slice.put(ck,  others);
		}
		if (slice.isEmpty())
			return null;
		// finally for column key in the partition, find expressions that can be mapped to it.
		ListOfPairs<ColumnKey,ExpressionNode> out = new ListOfPairs<ColumnKey,ExpressionNode>();
		for(Map.Entry<ColumnKey, Set<ColumnKey>> me : slice.entrySet()) {
			for(ColumnKey ock : me.getValue()) {
				Collection<ExpressionNode> exprs = restrictions.get(ock);
				if (exprs == null || exprs.isEmpty()) continue;
				for(ExpressionNode en : exprs) 
					out.add(me.getKey(),en);
			}
		}
		return out;
	}
	
	public ListOfPairs<ColumnKey,ExpressionNode> findNonPropagatingRestrictions(DPart part) {
		prune();
		ListOfPairs<ColumnKey,ExpressionNode> out = new ListOfPairs<ColumnKey,ExpressionNode>();
		for(ColumnKey ck : restrictions.keySet()) {
			if (part.getTables().contains(ck.getTableKey())) {
				for(ExpressionNode en : restrictions.get(ck))
					out.add(ck, en);
			}
		}
		return out;
	}
	
	public void describe(PrintStream ps) {
		ps.println("****** Restrictions **********");
		ps.println("Column forwarding:");
		HashSet<Pair<ColumnKey,ColumnKey>> processed = new HashSet<Pair<ColumnKey,ColumnKey>>();
		for(ColumnKey lk : restrictionForwarding.keySet()) {
			Set<ColumnKey> rks = (Set<ColumnKey>) restrictionForwarding.get(lk);
			for(ColumnKey rk : rks) {
				ColumnKey l, r;
				if (lk.getTableKey().getNode() < rk.getTableKey().getNode()) {
					l = lk;
					r = rk;
				} else {
					l = rk;
					r = lk;
				}
				Pair<ColumnKey,ColumnKey> proc = new Pair<ColumnKey,ColumnKey>(l,r);
				if (processed.add(proc)) {
					ps.println("  " + l + " <==> " + r);
				}
			}
		}
		ps.println();
		ps.println("Column restrictions: ");
		for(ColumnKey ck : restrictions.keySet()) {
			ps.println("  " + ck);
			for(ExpressionNode en : restrictions.get(ck)) {
				ps.println("      " + en);
			}
		}
		ps.println();
	}
	
	private void prune() {
		if (pruned) return;
		pruned = true;
		// any table that is null tested - remove from the restrictions
		if (nullTested.isEmpty()) return;
		List<ColumnKey> cks = Functional.toList(restrictions.keySet());
		for(ColumnKey ck : cks) {
			if (nullTested.contains(ck.getTableKey()))
				restrictions.remove(ck);
		}
	}
}
