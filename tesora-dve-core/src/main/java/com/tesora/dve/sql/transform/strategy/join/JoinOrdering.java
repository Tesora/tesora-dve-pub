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

import java.io.PrintStream;
import java.util.List;

import com.tesora.dve.sql.jg.CollapsedJoinGraph;
import com.tesora.dve.sql.jg.DGJoin;
import com.tesora.dve.sql.jg.JoinPosition;
import com.tesora.dve.sql.jg.UncollapsedJoinGraph;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.ResizableArray;

// we may have some ordering issues - i.e. within a branch we may be required to
// execute either forwards or backwards.  in the absence of dependency issues, we
// execute in whatever order is suitable.  if there are dependency issues, we must
// execute branches left to right, and left to right within branches.
class JoinOrdering {
	
	private CollapsedJoinGraph graph;
	private ResizableArray<BranchOrder> branchOrders = new ResizableArray<BranchOrder>();
	private JoinExecutionOrder overall = null;
	
	public JoinOrdering(CollapsedJoinGraph jg) {
		graph = jg;
		List<DGJoin> allJoins = graph.getNaturalOrder();
		for(Integer i : graph.getBranches().values()) 
			branchOrders.set(i, new BranchOrder());
		for(DGJoin dgj : allJoins) {
			JoinPosition jp = dgj.getPosition();
			if (jp.isInformal()) continue;
			BranchOrder bo = branchOrders.get(jp.getBranch());
			bo.addJoin(dgj);
			if (jp.isFixed() || dgj.isMultijoin()) {
				bo.setOrder(JoinExecutionOrder.LEFT);
				// this is possibly overly conservative 
				overall = JoinExecutionOrder.LEFT;
			}
		}
		
	}

	public boolean canschedule(SchemaContext sc, JoinEntry je) {
		DGJoin dgj = je.getJoin();
		ListSet<DGJoin> clustered = je.getClusteredJoins();
		// only true for cartesian joins
		if (dgj == null) return true;
		JoinPosition jp = dgj.getPosition();
		if (jp.isInformal()) return true;
		BranchOrder bo = branchOrders.get(jp.getBranch());
		if (dgj.isInnerJoin() && !dgj.isMultijoin()) {
			// I think inner joins, other than multijoins, can always be executed
			return bo.matches(dgj, clustered, null);
		}
		if (overall == JoinExecutionOrder.LEFT) {
			// we can only execute this join if it is the next in the natural order - and the natural order is the
			// first branch order with anything in it
			BranchOrder fbo = null;
			for(int i = 0; i < branchOrders.size(); i++) {
				BranchOrder ibo = branchOrders.get(i);
				if (ibo == null) continue;
				if (ibo.getNaturalOrder().isEmpty()) continue;
				fbo = ibo;
				break;
			}
			if (fbo != bo) return false;
			return bo.matches(dgj, clustered, JoinExecutionOrder.LEFT);
		} else if (bo.getOrder() == null) {
			if (bo.hasOuterJoins()) {
				// dgj must be either the first or the last - that will determine the order
				if (bo.matches(dgj, clustered, JoinExecutionOrder.LEFT)) {
					bo.setOrder(JoinExecutionOrder.LEFT);
					return true;
				} else if (bo.matches(dgj, clustered, JoinExecutionOrder.RIGHT)) {
					bo.setOrder(JoinExecutionOrder.RIGHT);
					return true;
				}
			} else {
				// has no outer joins, order doesn't matter at all - so go ahead and schedule it
				return bo.matches(dgj, clustered, null);
			}
		} else if (bo.matches(dgj, clustered, bo.getOrder())) {
			return true;
		}
		return false;
	}
	

	public void describe(SchemaContext sc, PrintStream out) {
		out.println("Uncollapsed join graph:");
		UncollapsedJoinGraph ujg = new UncollapsedJoinGraph(sc, graph.getStatement());
		out.println(ujg.describe(sc));
		out.println("Collapsed join graph:");
		out.println(graph.describe(sc));
		out.println("Branch orders:");
		for(int i = 0; i < branchOrders.size(); i++) {
			out.println("  [" + i + "]");
			for(DGJoin dgj : branchOrders.get(i).getNaturalOrder()) {
				StringBuilder buf = new StringBuilder();
				dgj.describe(sc,buf);
				buf.append(" ").append("{").append(dgj.getLeftTables()).append("} <=> {").append(dgj.getRightTable()).append("}");
				out.println("    " + buf.toString());
			}
		}
	}
	
	private static class BranchOrder {
		
		private JoinExecutionOrder order;
		private boolean hasOJs;
		private ListSet<DGJoin> naturalOrderOnBranch;
		
		public BranchOrder() {
			order = null;
			naturalOrderOnBranch = new ListSet<DGJoin>();
		}
		
		public void addJoin(DGJoin dgj) {
			naturalOrderOnBranch.add(dgj);
			if (!dgj.isInnerJoin())
				hasOJs = true;
		}
		
		public boolean hasOuterJoins() {
			return hasOJs;
		}
		
		public JoinExecutionOrder getOrder() {
			return order;
		}
		
		public void setOrder(JoinExecutionOrder jeo) {
			order = jeo;
		}		
		
		public ListSet<DGJoin> getNaturalOrder() {
			return naturalOrderOnBranch;
		}
		
		public boolean matches(DGJoin dgj, ListSet<DGJoin> clustered, JoinExecutionOrder jeo) {
			if (jeo == JoinExecutionOrder.LEFT) {
				if (matches(0,dgj,clustered))
					return true;
			} else if (jeo == JoinExecutionOrder.RIGHT) {
				if (matches(naturalOrderOnBranch.size() - 1,dgj,clustered))
					return true;
			} else if (jeo == null) {
				return naturalOrderOnBranch.remove(dgj);
			}
			return false;
		}				
		
		private boolean matches(int branchIndex, DGJoin given, ListSet<DGJoin> clustered) {
			DGJoin onBranch = naturalOrderOnBranch.get(branchIndex);
			if (onBranch == given) {
				naturalOrderOnBranch.remove(branchIndex);
				return true;
			} else if (clustered != null && clustered.contains(onBranch)) {
				naturalOrderOnBranch.removeAll(clustered);
				return true;
			}
			return false;
		}			
	}
	
}