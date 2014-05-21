// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.correlated;

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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tesora.dve.common.MultiMap;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.expression.ColumnKey;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ConstantExpression;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.Subquery;
import com.tesora.dve.sql.node.structural.SortingSpecification;
import com.tesora.dve.sql.node.test.EdgeTest;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.FunctionName;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.TempTable;
import com.tesora.dve.sql.statement.dml.DMLStatementUtils;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.ColumnInstanceCollector;
import com.tesora.dve.sql.transform.CopyVisitor;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.TransformFactory;
import com.tesora.dve.sql.transform.strategy.featureplan.RedistFeatureStep;
import com.tesora.dve.sql.util.ListOfPairs;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.util.Pair;

abstract class CorrelatedSubquery {
	
	protected Subquery sq;
	// these are the external columns within the query
	protected ListSet<ColumnKey> externalColumns;
	// these are the inner/outer column pairs for the joins - this will be used to build out
	// the lookup table; the inner columns are on the left
	protected ListOfPairs<ColumnKey,ColumnKey> joinConditions;
	// these are the inner/outer column pairs for joins that skip a level, i.e. if I have
	// select .. from A a where (a.id = (select min(b.id) from B b where a.fid = b.fid and b.sid = (select max(c.id) from C c where c.did = b.did and c.pid = a.pid)))
	// when evaluating the select min(b.id), joinConditions has a.fid=b.fid,
	// and mapConditions has c.pid=a.pid; again inner columns are on the left
	protected ListOfPairs<ColumnKey,ColumnKey> mapConditions;
	// the temp table we create
	protected TempTable tempTable;
	// where this subquery is
	protected EdgeTest location;
	// if this is an exists (subq)
	protected boolean exists;
	
	public CorrelatedSubquery(SchemaContext sc, Subquery sq, EdgeTest location, ListSet<ColumnKey> outerColumns) {
		this.sq = sq;
		this.location = location;
		this.externalColumns = outerColumns;
		this.joinConditions = new ListOfPairs<ColumnKey,ColumnKey>();
		this.mapConditions = new ListOfPairs<ColumnKey,ColumnKey>();
		ListSet<FunctionCall> equijoins = EngineConstant.EQUIJOINS.getValue(sq, sc);
		for(FunctionCall fc : equijoins) {
			// should make sure the function call is bounded by sq's statement, not a child statement
			ProjectingStatement parent = fc.getEnclosing(ProjectingStatement.class, null);
			ExpressionNode ln = ExpressionUtils.getUnaryColumnInstance(fc.getParametersEdge().get(0));
			ExpressionNode rn = ExpressionUtils.getUnaryColumnInstance(fc.getParametersEdge().get(1));
			if (!(ln instanceof ColumnInstance))
				continue;
			if (!(rn instanceof ColumnInstance))
				continue;
			ColumnInstance l = (ColumnInstance)ln; 
			ColumnInstance r = (ColumnInstance)rn;
			if (l == null || r == null)
				continue;
			ColumnKey lk = l.getColumnKey();
			ColumnKey rk = r.getColumnKey();
			if (outerColumns.contains(lk) && !outerColumns.contains(rk)) {
				if (parent != sq.getStatement())
					mapConditions.add(rk,lk);
				else
					joinConditions.add(rk,lk);
			} else if (outerColumns.contains(rk) && !outerColumns.contains(lk)) {
				if (parent != sq.getStatement())
					mapConditions.add(lk,rk);
				else
					joinConditions.add(lk,rk);	
			}
		}
		exists = EngineConstant.FUNCTION.has(sq.getParent(), EngineConstant.EXISTS);
	}
	
	public ListSet<ColumnKey> getOuterColumns() {
		return externalColumns;
	}
	
	// rhs is outer, lhs is inner
	public ListOfPairs<ColumnKey,ColumnKey> getJoinColumns() {
		return joinConditions;
	}
	
	public Subquery getSubquery() {
		return sq;
	}
	
	public void setTempTable(TempTable tt) {
		tempTable = tt;
	}
	
	public TempTable getTempTable() {
		return tempTable;
	}
	
	public EdgeTest getLocation() {
		return location;
	}
	
	public abstract SelectStatement pasteTempTable(PlannerContext pc, SelectStatement into, RedistFeatureStep tempTableStep) throws PEException;
	
	public abstract void removeFromParent(SelectStatement parent) throws PEException;
	
	public abstract int getOffset();
	
	public SelectStatement rewriteSubqueryUsingLookupTable(SchemaContext sc, TempTable lookupTable) throws PEException {
		SelectStatement ss = (SelectStatement) sq.getStatement();
		SelectStatement copy = CopyVisitor.copy(ss);
		SelectStatement ltq = lookupTable.buildSelect(sc);
		SelectStatement combined = DMLStatementUtils.compose(sc, copy, ltq);
		// remove everything from the ltq on the projection - it's not important
		for(int i = 0; i < ltq.getProjectionEdge().size(); i++)
			combined.getProjectionEdge().remove(combined.getProjectionEdge().size() - 1);

		// map forward all the outer join tables/columns in the combined
		// and map forward the outer join columns in the combined
		ListSet<ColumnInstance> allCols = ColumnInstanceCollector.getColumnInstances(combined);
		for(ColumnInstance ci : allCols) {
			ColumnKey ck = ci.getColumnKey();
			if (getOuterColumns().contains(ck)) {
				ColumnKey ltmck = ltq.getMapper().copyColumnKeyForward(ck);
				ColumnKey mck = combined.getMapper().copyColumnKeyForward(ltmck);
				ColumnInstance nci = mck.toInstance();
				ci.getParentEdge().set(nci);
			}
		}
		
		// we're going to add the group by now on the join condition if we have agg funs 
		for(Pair<ColumnKey,ColumnKey> p : getJoinColumns()) {
			ColumnKey cck = combined.getMapper().copyColumnKeyForward(p.getFirst());
			ColumnInstance ccki = cck.toInstance();
			if (!exists) {
				SortingSpecification gs = new SortingSpecification((ColumnInstance)ccki.copy(combined.getMapper().getCopyContext()),true);
				gs.setOrdering(Boolean.FALSE);
				combined.getGroupBysEdge().add(gs);
			}
			// also add it to the projection
			combined.getProjectionEdge().add(ccki);
		}
		combined.normalize(sc);

		return combined;
	}

	
	interface SimplifyPass {
		
		boolean simplify(CorrelatedSubquery csq, SchemaContext sc, TransformFactory xform);
		
		String getName();
		
	}

	// as the name suggests, if you have something like ... in (select max(id) from A limit 1)
	// this removes the limit 1, since it is redundant.
	protected static final SimplifyPass stripRedundantLimit = new SimplifyPass() {

		@Override
		public String getName() {
			return "StripRedundantLimit";
		}
		
		@Override
		public boolean simplify(CorrelatedSubquery csq, SchemaContext sc,
				TransformFactory xform) {
			if (!(csq.sq.getStatement() instanceof SelectStatement))
				return false;
			ProjectingStatement ss = csq.sq.getStatement();
			ListSet<FunctionCall> projAggFun = EngineConstant.PROJ_AGGREGATE_FUNCTIONS.getValue(ss,sc);
			boolean hasLimitOne = (csq.sq.getStatement().getLimit() == null) ? false : csq.sq.getStatement().getLimit().hasLimitOne(sc);
			
			if (!projAggFun.isEmpty() && hasLimitOne) {
				// can filter the limit 1
				ss.getLimitEdge().clear();
				if (xform.emitting()) {
					xform.emit("removed redundant limit 1");
				}
				return true;
			}
			return false;
		}
	};
	
	// if I have subq G, subq P, subq C where C is nested in P, and P is nested in G
	// and C is correlated to both P and G, this simplification attempts to make C be correlated only to P.
	// note as well that this only handles immediate children; grandchildren are handled recursively
	protected static final SimplifyPass forwardDeeplyCorrelated = new SimplifyPass() {

		@Override
		public String getName() {
			return "ForwardDeeplyCorrelated";
		}
		
		@Override
		public boolean simplify(CorrelatedSubquery csq, SchemaContext sc,
				TransformFactory xform) {
			if (csq.mapConditions.isEmpty()) return false;
			if (!(csq.sq.getStatement() instanceof SelectStatement))
				return false;
			ProjectingStatement ss = csq.sq.getStatement();
			String before = null;
			if (xform.emitting()) 
				before = ss.getSQL(sc,"  ");
			// so we have join conditions with inner cols on the left and outer cols on the right
			// and we have map conditions with inner cols (nested) on the left and outer cols on the right
			// we need to build a forward map from nested outer cols to parent inner cols
			// so build a reverse map for outer cols to parent inner cols
			HashMap<ColumnKey,ColumnKey> lookup = new HashMap<ColumnKey,ColumnKey>();
			for(Pair<ColumnKey,ColumnKey> p : csq.joinConditions) 
				lookup.put(p.getSecond(), p.getFirst());
			HashMap<ColumnKey,ColumnKey> forwarding = new HashMap<ColumnKey,ColumnKey>();
			for(Pair<ColumnKey,ColumnKey> p : csq.mapConditions) {
				ColumnKey jump = lookup.get(p.getSecond());
				if (jump != null)
					forwarding.put(p.getSecond(),jump);
			}
			boolean any = false;
			ListSet<ProjectingStatement> nested = ss.getDerivedInfo().getLocalNestedQueries();
			for(ProjectingStatement ps : nested) {
				ListSet<FunctionCall> equijoins = EngineConstant.EQUIJOINS.getValue(ps, sc);
				for(FunctionCall fc : equijoins) {
					ProjectingStatement parent = fc.getEnclosing(ProjectingStatement.class, null);
					if (parent != ps) continue;
					if (mapDeepCorrelated((ColumnInstance) fc.getParametersEdge().get(0),forwarding))
						any = true;
					if (mapDeepCorrelated((ColumnInstance) fc.getParametersEdge().get(1),forwarding))
						any = true;
				}
			}
			if (any && xform.emitting()) {
				xform.emit("deeply correlated forwarding:");
				xform.emit("before: " + before);
				xform.emit("after: " + ss.getSQL(sc, "  "));
			}
			return any;
		}

		
		private boolean mapDeepCorrelated(ColumnInstance ici, Map<ColumnKey,ColumnKey> lookup) {
			ColumnKey ck = ici.getColumnKey();
			ColumnKey redirect = lookup.get(ck);
			if (redirect == null) return false;
			ColumnInstance nci = redirect.toInstance();
			ici.getParentEdge().set(nci);
			return true;
		}

	};

	// suppose I have 
	// A a where a.id in ( select max(b.id) from B b where b.fid = a.fid and b.fid = 22) 
	// we know from constant propagation that this is really
	// A a where a.id in ( select max(b.id) from B b where b.fid = 22 )
	// so arrange for that to be the case.  this only involves looking at the where clause for now
	// (we will do join conditions in the future).
	protected static final SimplifyPass propagateFixedCorrelation = new SimplifyPass() {

		@Override
		public String getName() {
			return "PropagateFixedCorrelation";
		}
		
		@Override
		public boolean simplify(CorrelatedSubquery csq, SchemaContext sc,
				TransformFactory xform) {
			if (!(csq.sq.getStatement() instanceof SelectStatement))
				return false;
			ProjectingStatement ss = csq.sq.getStatement();
			ExpressionNode root = ss.getWhereClause();
			if (root == null) 
				return false;
			String before = null;
			if (xform.emitting())
				before = ss.getSQL(sc, "  ");
			List<ExpressionNode> decompAnd = ExpressionUtils.decomposeAndClause(ss.getWhereClause());			
			MultiMap<ColumnKey,ExpressionNode> rhsForColumn = new MultiMap<ColumnKey,ExpressionNode>();
			Map<ExpressionNode,ExpressionNode> parentage = new HashMap<ExpressionNode,ExpressionNode>();
			buildEqualityBits(decompAnd,rhsForColumn,parentage);
			boolean any = false;
			for(ColumnKey ck : rhsForColumn.keySet()) {
				List<ExpressionNode> sub = (List<ExpressionNode>) rhsForColumn.get(ck);
				if (sub == null || sub.isEmpty()) continue;
				if (sub.size() == 1) continue;
				ExpressionNode constant = null;
				List<ExpressionNode> nonconstant = new ArrayList<ExpressionNode>();
				for(ExpressionNode en : sub) {
					if (en instanceof ConstantExpression) {
						constant = en;
					} else {
						nonconstant.add(en);
					}
				}
				if (constant != null) {
					// remove all the nonconstant versions. 
					for(ExpressionNode en : nonconstant) {
						ExpressionNode p = parentage.get(en);
						decompAnd.remove(p);
						any = true;
					}
				}
			}
			if (any) {
				ss.setWhereClause(ExpressionUtils.safeBuildAnd(decompAnd));
				if (xform.emitting()) {
					xform.emit("fixed correlated propagation:");
					xform.emit("before: " + before);
					xform.emit("after: " + ss.getSQL(sc, "  "));
				}
				return true;
			}
			
			return false;
		}
		
	};
	
	private static void buildEqualityBits(List<ExpressionNode> decompAnd, MultiMap<ColumnKey,ExpressionNode> rhsForColumn,
			Map<ExpressionNode,ExpressionNode> parentage) {
		for(ExpressionNode en : decompAnd) {
			if (EngineConstant.FUNCTION.has(en, EngineConstant.EQUALS)) {
				FunctionCall fc = (FunctionCall) en;
				ExpressionNode lhs = fc.getParametersEdge().get(0);
				ExpressionNode rhs = fc.getParametersEdge().get(1);
				if (lhs instanceof ColumnInstance) {
					ColumnInstance ci = (ColumnInstance) lhs;
					rhsForColumn.put(ci.getColumnKey(), rhs);
					parentage.put(rhs,en);
				}
			}
		}

	}
	
	// if we have from A a where a.id in ( select max(id) from B b where a.fid = b.fid ) and a.fid = 22
	// we can rewrite this to A a where a.id in (select max(id) from B b where b.fid = 22)
	// actually, we just yank the b.fid down, and then let the correlated propagation pass take care of the rest
	// note that we only do this for where clause subqs
	// if we do it for proj subqs then we can remove the correlation (maybe) and turn a cor subq into a regular subq
	protected static final SimplifyPass yankDownRestrictions = new SimplifyPass() {

		
		@Override
		public String getName() {
			return "YankDownRestrictions";
		}
		
		@Override
		public boolean simplify(CorrelatedSubquery csq, SchemaContext sc,
				TransformFactory xform) {
			if (!(csq.sq.getStatement() instanceof SelectStatement))
				return false;
			SelectStatement ss = (SelectStatement) csq.sq.getStatement();
			ProjectingStatement parent = csq.sq.getEnclosing(ProjectingStatement.class, null);
			if (!(parent instanceof SelectStatement))
				return false;
			SelectStatement pss = (SelectStatement) parent;
			List<ExpressionNode> decompAnd = ExpressionUtils.decomposeAndClause(pss.getWhereClause());
			MultiMap<ColumnKey,ExpressionNode> rhsForColumn = new MultiMap<ColumnKey,ExpressionNode>();
			Map<ExpressionNode,ExpressionNode> parentage = new HashMap<ExpressionNode,ExpressionNode>();
			buildEqualityBits(decompAnd,rhsForColumn,parentage);
			List<ExpressionNode> cdecomp = ExpressionUtils.decomposeAndClause(ss.getWhereClause());
			MultiMap<ColumnKey,ExpressionNode> childRhsForColumn = new MultiMap<ColumnKey,ExpressionNode>();
			buildEqualityBits(cdecomp,childRhsForColumn,new HashMap<ExpressionNode,ExpressionNode>());
			HashMap<ColumnKey,ConstantExpression> existing = new HashMap<ColumnKey,ConstantExpression>();
			for(ColumnKey ck : childRhsForColumn.keySet()) {
				Collection<ExpressionNode> sub = childRhsForColumn.get(ck);
				if (sub == null || sub.isEmpty()) continue;
				for(ExpressionNode cen : sub) {
					if (cen instanceof ConstantExpression) {
						existing.put(ck,(ConstantExpression)cen);
					}
				}
			}
			List<ExpressionNode> addenda = new ArrayList<ExpressionNode>();
			for(Pair<ColumnKey,ColumnKey> p : csq.getJoinColumns()) {
				ExpressionNode restricted = existing.get(p.getFirst());
				if (restricted != null) continue;
				List<ExpressionNode> sub = (List<ExpressionNode>) rhsForColumn.get(p.getSecond());
				if (sub == null || sub.isEmpty()) continue;
				for(ExpressionNode en : sub) {
					if (en instanceof ConstantExpression) {
						addenda.add(new FunctionCall(FunctionName.makeEquals(),p.getFirst().toInstance(),(ExpressionNode)en.copy(null)));
					}
				}
			}
			if (addenda.isEmpty())
				return false;
			List<ExpressionNode> wc = ExpressionUtils.decomposeAndClause(ss.getWhereClause());
			wc.addAll(addenda);
			ss.setWhereClause(ExpressionUtils.safeBuildAnd(wc));
			return true;
		}
		
	};
	
	// we have a correlated subquery that looks like
	// (select a.fid from A a where a.id = b.id and a.id = 15)
	// we can uncorrelate this by removing the "a.id = b.id" bit since a.id is forced to be 15 anyhow
	protected static final SimplifyPass removeCorrelation = new SimplifyPass() {

		@Override
		public String getName() {
			return "RemoveCorrelation";
		}
		
		@Override
		public boolean simplify(CorrelatedSubquery csq, SchemaContext sc,
				TransformFactory xform) {
			if (!(csq.sq.getStatement() instanceof SelectStatement))
				return false;
			SelectStatement ss = (SelectStatement)csq.sq.getStatement();
			ListSet<ExpressionNode> decompAnd = ExpressionUtils.decomposeAndClause(ss.getWhereClause());
			MultiMap<ColumnKey,FunctionCall> nonConstantEqs = new MultiMap<ColumnKey,FunctionCall>();
			MultiMap<ColumnKey,FunctionCall> constantEqs = new MultiMap<ColumnKey,FunctionCall>();
			for(ExpressionNode en : decompAnd) {
				if (EngineConstant.FUNCTION.has(en, EngineConstant.EQUALS)) {
					FunctionCall fc = (FunctionCall) en;
					if (fc.getParametersEdge().get(0) instanceof ColumnInstance) {
						ColumnInstance ci = (ColumnInstance) fc.getParametersEdge().get(0);
						if (EngineConstant.CONSTANT.has(fc.getParametersEdge().get(1))) {
							constantEqs.put(ci.getColumnKey(),fc);
						} else {
							if (fc.getParametersEdge().get(1) instanceof ColumnInstance) {
								ColumnInstance oci = (ColumnInstance) fc.getParametersEdge().get(1);
								nonConstantEqs.put(oci.getColumnKey(), fc);
								nonConstantEqs.put(ci.getColumnKey(), fc);
							} else {
								nonConstantEqs.put(ci.getColumnKey(),fc);
							}
						}
					}
				}
			}
			if (constantEqs.isEmpty())
				return false;
			int before = decompAnd.size();
			for(ColumnKey ck : constantEqs.keySet()) {
				Collection<FunctionCall> sub = constantEqs.get(ck);
				if (sub == null || sub.isEmpty()) continue;
				Collection<FunctionCall> oth = nonConstantEqs.get(ck);
				if (oth == null || oth.isEmpty()) continue;
				// for the stuff we're going to remove, we remove only if the expr contains
				// columns from this and the outer query
				for(FunctionCall fc : oth) {
					ListSet<TableKey> tabs = ColumnInstanceCollector.getTableKeys(fc);
					boolean encapsulated = true;
					for(TableKey tk : tabs) {
						if (!ss.getDerivedInfo().getAllTableKeys().contains(tk))
							encapsulated = false;
					}
					if (!encapsulated)
						decompAnd.remove(fc);
				}
			}
			ss.setWhereClause(ExpressionUtils.safeBuildAnd(decompAnd));
			return decompAnd.size() != before;			
		}
		
	};
	
	
	protected abstract SimplifyPass[] getSimplifications(); 
	
	public final boolean simplify(SchemaContext sc, TransformFactory xform) {
		boolean global = false;
		SimplifyPass[] passes = getSimplifications();
		// always single pass
		for(SimplifyPass sp : passes) {
			if (sp.simplify(this, sc, xform)) {
				global = true;
			}
		}
		return global;
	}
	

}