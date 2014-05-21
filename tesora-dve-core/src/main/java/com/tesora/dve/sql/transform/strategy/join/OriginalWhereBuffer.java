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
import java.util.List;

import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.jg.DPart;
import com.tesora.dve.sql.jg.JoinEdge;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.ColumnInstanceCollector;
import com.tesora.dve.sql.transform.FunCollector;
import com.tesora.dve.sql.transform.NullFunCollector;
import com.tesora.dve.sql.util.Functional;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.UnaryPredicate;

class OriginalWhereBuffer extends FinalBuffer {

	protected PartitionLookup partitionLookup;
	
	public OriginalWhereBuffer(Buffer prev, PartitionLookup pl) {
		super(BufferKind.WC, prev,pl);
		partitionLookup = pl;
	}
	
	@Override
	public void adapt(final SchemaContext sc, SelectStatement stmt) {
		P2ProjectionBuffer proj = (P2ProjectionBuffer) getBuffer(BufferKind.P2);
		// see if we have any outer joins
		ListSet<TableKey> allOuterJoinedTables = new ListSet<TableKey>();
		ListSet<TableKey> outerJoinedTables = new ListSet<TableKey>(); 
		for(JoinEdge je : partitionLookup.jg.getJoins()) {
			if (je.getJoin().isInnerJoin()) continue;
			allOuterJoinedTables.add(je.getLHSTab());
			allOuterJoinedTables.add(je.getRHSTab());
			if (je.getJoinType().isLeftOuterJoin())
				outerJoinedTables.add(je.getRHSTab());
			else if (je.getJoinType().isRightOuterJoin())
				outerJoinedTables.add(je.getLHSTab());
			else {
				outerJoinedTables.add(je.getLHSTab());
				outerJoinedTables.add(je.getRHSTab());
			}
		}

		List<ExpressionNode> clauses = ExpressionUtils.decomposeAndClause(stmt.getWhereClause());
		
		// so, null handling:
		// there are two cases where we need to delay processing of the where clause to after
		// the join has been performed:
		// [1] is null, is not null
		// [2] where clauses utilizing columns in the outer joined table
		// in the first case, we're going to delay processing the predicate until after the tables are joined
		// in the second case we're going to process the predicate twice: once for the partition query and again
		// after the join is done.  
		
		for(ExpressionNode en : clauses) {
			partitionLookup.getRestrictionManager().take(en);
			ListSet<ColumnInstance> columns = ColumnInstanceCollector.getColumnInstances(en);
			ListSet<TableKey> tabs = ColumnInstanceCollector.getTableKeys(columns);

			ListSet<TableKey> both = null;
			
			ListSet<FunctionCall> funs = FunCollector.collectFuns(en);
			
			if (!allOuterJoinedTables.isEmpty()) {
				List<FunctionCall> nullFuns = Functional.select(funs, NullFunCollector.isNullFun);
				if (!nullFuns.isEmpty()) {
					// we're going to add all the outer joined stuff to the dependencies, just to be safe
					// this means we'll delay doing the is null checks until the joins are planned
					tabs.addAll(allOuterJoinedTables);
				} else {
					ListSet<ColumnInstance> cols = ColumnInstanceCollector.getColumnInstances(en);
					ListSet<TableKey> tks = ColumnInstanceCollector.getTableKeys(cols);
					for(TableKey tk : tks) {
						if (outerJoinedTables.contains(tk)) {
							both = allOuterJoinedTables;
							break;
						}
					}
				}
			}
			List<FunctionCall> eqjs = Functional.select(funs, new UnaryPredicate<FunctionCall>() {

				@Override
				public boolean test(FunctionCall ln) {
					Boolean value = ln.getDerivedAttribute(EngineConstant.EQUIJOIN, sc);
					if (value == null) return false;
					return value.booleanValue();
				}
				
			});
			
			if (both != null) {
				ListSet<DPart> partitions = partitionLookup.getPartitionsFor(tabs);
				if (partitions.size() <= 1)
					addNonBridging(partitions,en);
				partitions = partitionLookup.getPartitionsFor(both);
				addBridgingDependency(partitions, en, columns, proj, eqjs, sc);				
			} else {
				ListSet<DPart> partitions = partitionLookup.getPartitionsFor(tabs);
				if (partitions.size() > 1) {
					addBridgingDependency(partitions, en, columns, proj, eqjs, sc);
				} else {
					addNonBridging(partitions,en);
				}
			}
		}
	}
	
	private void addBridgingDependency(ListSet<DPart> partitions, ExpressionNode en, ListSet<ColumnInstance> columns,
			P2ProjectionBuffer proj, List<FunctionCall> eqjs, SchemaContext sc) {
		// figure out if we have any redist entries
		List<ExpressionNode> redistEntries = new ArrayList<ExpressionNode>();
		for(FunctionCall fc : eqjs) {
			for(ExpressionNode p : fc.getParametersEdge()) {
				if (!(p instanceof ColumnInstance)) {
					redistEntries.add(p);
				}
			}
		}
		ListSet<ExpressionNode> bits = new ListSet<ExpressionNode>();
		ListSet<ColumnInstance> splodeyCols = new ListSet<ColumnInstance>();
		if (redistEntries.isEmpty()) {
			for(ColumnInstance ci : columns)
				bits.add(ci);
			splodeyCols = columns;
		} else {
			for(ColumnInstance ci : columns) {
				if (ci.ifAncestor(redistEntries) == null) {
					bits.add(ci);
					splodeyCols.add(ci);
				}
			}
			bits.addAll(redistEntries);
		}		
		// bridging, we need to decompose
		// but we also need to add entries to the projection buffer, which is what was passed in
		BufferEntry be = new ExplodingBufferEntry(en, bits);
		for(ExpressionNode p : redistEntries) {
			BufferEntry nbe = proj.addForJoin(p, true);
			be.addDependency(nbe);
			nbe.registerCompoundRedist(p, (RedistBufferEntry) nbe);
			proj.partitionInfo.add(nbe,partitions);
		}
		for(ColumnInstance ci : splodeyCols) {
			BufferEntry de = proj.add(ci);
			be.addDependency(de);
		}
		add(be);
		partitionInfo.add(be, partitions);	
	}
	
	private void addNonBridging(ListSet<DPart> partitions, ExpressionNode en) {
		BufferEntry be = new BufferEntry(en);
		add(be);
		partitionInfo.add(be, partitions);		
	}
}	
