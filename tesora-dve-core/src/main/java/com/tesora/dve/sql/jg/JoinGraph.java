// OS_STATUS: public
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.tesora.dve.sql.SchemaException;
import com.tesora.dve.sql.ParserException.Pass;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.MultiTableDMLStatement;

public abstract class JoinGraph {

	private static final boolean emitState = Boolean.getBoolean("JOIN_GRAPH");
	
	MultiTableDMLStatement stmt;
	List<DPart> vertices;
	List<JoinEdge> edges;

	protected JoinGraph(MultiTableDMLStatement stmt) {
		vertices = new ArrayList<DPart>();
		edges = new ArrayList<JoinEdge>();
		this.stmt = stmt;
	}
	
	public String describe(SchemaContext sc) {
		StringBuilder buf = new StringBuilder();
		for(DPart dp : vertices) 
			dp.describe(sc,"", buf);
		return buf.toString();
	}
	
	protected void unhandled(String what) {
		throw new SchemaException(Pass.PLANNER, "Unhandled in " + this.getClass().getSimpleName() + ": " + what);
	}

	public List<JoinEdge> getJoins() {
		return edges;
	}
	
	public List<DPart> getPartitions() {
		return vertices;
	}
	
	public MultiTableDMLStatement getStatement() {
		return stmt;
	}

	public boolean isDegenerate() {
		return true;
	}
	
	public boolean requiresRedistribution() {
		return true;
	}
	
	public abstract Map<FromTableReference,Integer> getBranches();
	
	// natural order is left to right with informal wc joins at the end 
	public List<DGJoin> getNaturalOrder() {
		TreeMap<JoinPosition,DGJoin> byPosition = new TreeMap<JoinPosition,DGJoin>();
		HashSet<DGJoin> processed = new HashSet<DGJoin>();
		List<DGJoin> informal = new ArrayList<DGJoin>();
		for(JoinEdge je : edges) {
			if (!processed.add(je.getJoin())) continue;
			if (je.getJoin().getPosition().isInformal()) 
				informal.add(je.getJoin());
			byPosition.put(je.getJoin().getPosition(),je.getJoin());
		}
		ArrayList<DGJoin> out = new ArrayList<DGJoin>(byPosition.values());
		out.addAll(informal);
		return out;
	}
	
	public DPart getPartitionFor(TableKey tk) {
		for(DPart dp : vertices) {
			if (dp.getTables().contains(tk))
				return dp;
		}
		return null;
	}

	protected boolean emitting() {
		return emitState;
	}
	
	protected void emit(String what) {
		System.out.println(this.getClass().getSimpleName() + ": " + what);
	}
}
