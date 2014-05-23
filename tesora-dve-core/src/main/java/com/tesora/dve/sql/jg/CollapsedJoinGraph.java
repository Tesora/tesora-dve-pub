package com.tesora.dve.sql.jg;

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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.tesora.dve.common.LinkedHashSetFactory;
import com.tesora.dve.common.MultiMap;
import com.tesora.dve.common.PEConstants;
import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.ColumnKey;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.MultiTableDMLStatement;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.Pair;
import com.tesora.dve.sql.util.UnaryFunction;

public class CollapsedJoinGraph extends JoinGraph {

	protected UncollapsedJoinGraph src;
	
	public CollapsedJoinGraph(SchemaContext sc, MultiTableDMLStatement dmls) {
		super(dmls);
		src = new UncollapsedJoinGraph(sc, dmls);
		build(sc);
	}
	
	public CollapsedJoinGraph(SchemaContext sc, UncollapsedJoinGraph ujg) {
		super(ujg.getStatement());
		src = ujg;
		build(sc);
	}
	
	@Override
	public boolean requiresRedistribution() {
		return vertices.size() > 1;
	}
	
	@Override
	public List<DPart> getPartitions() {
		return vertices;
	}

	@Override
	public boolean isDegenerate() {
		return false;
	}

	public UncollapsedJoinGraph getUncollapsed() {
		return src;
	}
	
	protected void build(SchemaContext sc) {
		// construct a new graph out of the old graph but keep the graph ids in order to preserve declaration order
		HashMap<DPart,DMPart> containing = new HashMap<DPart,DMPart>();
		List<DMPart> newparts = new ArrayList<DMPart>();
		List<JoinEdge> joins = new ArrayList<JoinEdge>(src.getJoins());
		boolean anything;
		do {
			anything = false;
			anything = collapseUnaryJoins(sc,joins,containing,newparts,src.getPartitions().size());
			if (!anything)
				anything = collapseMultiJoins(sc,joins,containing,newparts,src.getPartitions().size());
		} while(anything);
		// Each table can be part of at most 1 partition.
		HashMap<TableKey,DMPart> allTab = new HashMap<TableKey,DMPart>();
		for(DMPart dmp : newparts) {
			for(TableKey tk : dmp.getTables()) {
				DMPart already = allTab.put(tk,dmp);
				if (already != null)
					throw new SchemaException(Pass.PLANNER, "Invalid collapsed join graph.  Table " + tk + " is part of partition " + already.describe(sc, "") + " and " + dmp.describe(sc, ""));
			}
		}
		
		// we have collapsed multiple partitions from the old graph into single partitions in the new graph
		// update edges to use collapsed partitions
		for(JoinEdge je : joins) {
			DMPart any = containing.get(je.getFrom());
			if (any != null) {
				je.setFrom(any);
				any.addEdge(je);
			}
			any = containing.get(je.getTo());
			if (any != null) {
				je.setTo(any);
				any.addEdge(je);
			}
		}
		edges = joins;
		List<DPart> oldParts = new ArrayList<DPart>(src.getPartitions());
		for(Iterator<DPart> iter = oldParts.iterator(); iter.hasNext();) {
			DPart dp = iter.next();
			if (containing.containsKey(dp))
				iter.remove();
		}
		vertices.addAll(oldParts);
		vertices.addAll(newparts);
	}

	private boolean collapseUnaryJoins(final SchemaContext sc, List<JoinEdge> joins, Map<DPart,DMPart> containing, List<DMPart> newparts, int srcParts) {
		int before = joins.size();
		int counter = 0;
		for(Iterator<JoinEdge> iter = joins.iterator(); iter.hasNext();) {
			JoinEdge je = iter.next();
			if (je.getJoin().isMultijoin()) continue;
			if (!je.isColocated(sc)) continue;
			iter.remove();
			if (emitting()) {
				emit(">>>>>> collapseUnaryJoins pass " + (++counter) + " <<<<<<<<<");
				emit(je.toString());
			}
			DMPart fcandidate = containing.get(je.getFrom());
			DMPart tcandidate = containing.get(je.getTo());
			DMPart candidate = null;
			if (fcandidate == null && tcandidate == null) {
				candidate = new DMPart(newparts.size() + srcParts);
				newparts.add(candidate);				
			} else if (fcandidate != null && tcandidate != null) {
				candidate = new DMPart(newparts.size() + srcParts);
				newparts.add(candidate);
				newparts.remove(fcandidate);
				newparts.remove(tcandidate);
				containing.remove(fcandidate);
				containing.remove(tcandidate);
				for(DPart dp : fcandidate.getUnaryParts()) {
					candidate.take(dp);
				}
				for(DPart dp : tcandidate.getUnaryParts()) {
					candidate.take(dp);
				}
				updateForwarding(candidate,containing,fcandidate);
				updateForwarding(candidate,containing,tcandidate);
			} else if (fcandidate != null) {
				candidate = fcandidate;
			} else {
				candidate = tcandidate;
			}
			candidate.take(je);
			containing.put(je.getFrom(), candidate);
			containing.put(je.getTo(), candidate);
			containing.put(candidate, candidate);
			je.getFrom().update(candidate);
			je.getTo().update(candidate);
			if (emitting()) {
				emit("newparts:");
				emit(Functional.join(newparts, PEConstants.LINE_SEPARATOR, new UnaryFunction<String, DMPart>() {

					@Override
					public String evaluate(DMPart object) {
						return "  " + object.terseDescribe(sc);
					}

				}));
				emit("containing:");
				for(Map.Entry<DPart, DMPart> me : containing.entrySet()) {
					emit("  " + me.getKey().terseDescribe(sc) + " => " + me.getValue().terseDescribe(sc));
				}
			}
		}
		return (before != joins.size());
	}		
	
	private static void updateForwarding(DMPart newCandidate, Map<DPart,DMPart> containing, DMPart oldCandidate) {
		for(JoinEdge ije : oldCandidate.getEmbeddedJoins()) {
			containing.put(ije.getFrom(), newCandidate);
			containing.put(ije.getTo(), newCandidate);
			ije.getFrom().update(newCandidate);
			ije.getTo().update(newCandidate);					
		}
	}
	
	private boolean collapseMultiJoins(SchemaContext sc, List<JoinEdge> joins, Map<DPart,DMPart> containing, List<DMPart> newparts, int srcParts) {
		int before = joins.size();
		HashSet<JoinEdge> toRemove = new HashSet<JoinEdge>();
		for(Iterator<JoinEdge> iter = joins.iterator(); iter.hasNext();) {
			JoinEdge je = iter.next();
			if (!je.getJoin().isMultijoin()) continue;
			if (toRemove.contains(je)) {
				iter.remove();
				continue;
			}
			DGJoin mj = je.getJoin();
			boolean colo = true;
			// in addition to external edges being colocated, internal edges between partitions within
			// the multijoin must also be colocated
			ListSet<DunPart> parts = new ListSet<DunPart>();			
			for(JoinEdge ije : mj.getEdges()) {
				if (!ije.isColocated(sc)) {
					colo = false;
					break;
				}
				parts.addAll(ije.getFrom().getUnaryParts());
				parts.addAll(ije.getTo().getUnaryParts());
			}
			if (!colo)
				continue;
			ListSet<JoinEdge> pertinent = new ListSet<JoinEdge>();
			for(DunPart dp : parts) {
				for(JoinEdge ije : dp.getEdges()) {
					if (parts.contains(ije.getFrom()) && parts.contains(ije.getTo()))
						pertinent.add(ije);
				}
			}
			for(JoinEdge ije : pertinent) {
				if (!ije.isColocated(sc)) {
					colo = false;
					break;
				}
			}
			if (!colo)
				continue;
			iter.remove();
			toRemove.addAll(pertinent);
			DMPart candidate = null;
			for(DunPart dp : parts) {
				candidate = containing.get(dp);
				if (candidate != null)
					break;
			}
			if (candidate == null) {
				candidate = new DMPart(newparts.size() + srcParts);
				newparts.add(candidate);
			}
			for(DunPart dp : parts)
				containing.put(dp, candidate);
			for(JoinEdge  ije : pertinent)
				candidate.take(ije);
			candidate.take(je);
		}
		return joins.size() != before;
	}

	@Override
	public Map<FromTableReference, Integer> getBranches() {
		return src.getBranches();
	}

	public MultiMap<ColumnKey,ColumnKey> getRestrictionPropagationMap() {
		MultiMap<ColumnKey,ColumnKey> out = new MultiMap<ColumnKey,ColumnKey>(new LinkedHashSetFactory<ColumnKey>());
		for(JoinEdge je : getJoins()) 
			collectJoinMapping(je,out);
		for(DPart dp : getPartitions()) {
			for(JoinEdge je : dp.getEmbeddedJoins()) {
				collectJoinMapping(je,out);
			}
		}
			
		return out;
	}

	private void collectJoinMapping(JoinEdge je, MultiMap<ColumnKey, ColumnKey> acc) {
		for(Pair<ColumnInstance,ColumnInstance> p : je.getSimpleColumns()) {
			ColumnKey l = p.getFirst().getColumnKey();
			ColumnKey r = p.getSecond().getColumnKey();
			acc.put(l,r);
			acc.put(r,l);
		}
	}
	
	public ListSet<DGJoin> getAllJoins() {
		ListSet<DGJoin> out = new ListSet<DGJoin>();
		for(JoinEdge je : getJoins())
			out.add(je.getJoin());
		for(DPart dp : getPartitions()) {
			for(JoinEdge je : dp.getEmbeddedJoins()) {
				out.add(je.getJoin());
			}
		}
		return out;
	}
	
}
