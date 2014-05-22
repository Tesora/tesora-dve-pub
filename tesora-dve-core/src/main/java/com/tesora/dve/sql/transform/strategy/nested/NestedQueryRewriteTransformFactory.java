package com.tesora.dve.sql.transform.strategy.nested;

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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.expression.ExpressionPath;
import com.tesora.dve.sql.node.EdgeName;
import com.tesora.dve.sql.node.expression.Subquery;
import com.tesora.dve.sql.node.test.EdgeTest;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.DeleteStatement;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.statement.dml.UpdateStatement;
import com.tesora.dve.sql.transform.CopyVisitor;
import com.tesora.dve.sql.transform.behaviors.ComplexFeaturePlannerFilter;
import com.tesora.dve.sql.transform.behaviors.defaults.DefaultFeaturePlannerFilter;
import com.tesora.dve.sql.transform.strategy.FeaturePlannerIdentifier;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.SingleSiteStorageGroupTransformFactory;
import com.tesora.dve.sql.transform.strategy.TransformFactory;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.util.ListSet;

/*
 * Handles uncorrelated subqueries on the projection, from or where clauses, or in update expressions.
 * The general strategy is to either rewrite the subquery as a join and plan the result, or else
 * to convert the subquery to a temp table, paste back into the parent query, and plan that.
 */
public class NestedQueryRewriteTransformFactory extends TransformFactory {

	private static final EdgeTest[] supportedLocations = 
			new EdgeTest[] { EngineConstant.FROMCLAUSE, EngineConstant.UPDATECLAUSE, EngineConstant.WHERECLAUSE, EngineConstant.PROJECTION };

	private static final StrategyFactory[] strategies =	new StrategyFactory[] {
		new HandleWhereClauseSubqueryAsJoin(),
		new HandleFromClauseSubquery(),
		new HandleScalarSubquery()
	};

	private static boolean applies(SchemaContext sc, DMLStatement stmt) throws PEException {

		if ((stmt instanceof ProjectingStatement)
				|| (stmt instanceof UpdateStatement)
				|| (stmt instanceof DeleteStatement)) {

			// nested queries are ok if they are on a single site - just push
			// down then
			if (SingleSiteStorageGroupTransformFactory.isSingleSite(sc, stmt, true))
				return false;
			ListSet<ProjectingStatement> any = EngineConstant.NESTED.getValue(stmt, sc);
			if (any.isEmpty() || (any.size() == 1 && any.get(0) == stmt))
				return false;// no subqueries provided.

			// statement has one or more nested queries that can't be pushed
			// down as is, choose a strategy to handle it, but only if the query
			// is not correlated.
			boolean nc = false;
			for (ProjectingStatement ps : any) {
				if (ps.getDerivedInfo().getCorrelatedColumns().isEmpty()) {
					nc = true;
					break;
				}
			}
			return nc;

		}

		return false;
	}
	
	// the role of this method is not only to find queries that this instance of this transform can handle, but to
	// figure how they can be handled
	private void classifySubqueries(SchemaContext sc, DMLStatement in, ListSet<NestingStrategy> subs) throws PEException {
		ListSet<ProjectingStatement> nested = EngineConstant.NESTED.getValue(in,sc);

		// first pass, determine ancestry chains
		LinkedHashMap<ProjectingStatement, DMLStatement> ancestry = new LinkedHashMap<ProjectingStatement, DMLStatement>();
		for(ProjectingStatement ss : nested) {
			DMLStatement firstAncestor = ss.getParent().getEnclosing(DMLStatement.class, null);
			if (firstAncestor == null) continue;
			ancestry.put(ss,firstAncestor);
		}
		// now, we only want to look at those queries for which firstAncestor==in
		
		for (Map.Entry<ProjectingStatement, DMLStatement> me : ancestry.entrySet()) {
			if (me.getValue() != in) continue; // the ancestor of this select is not the stmt we were given
			ProjectingStatement ns = me.getKey();
			if (!ns.getDerivedInfo().getCorrelatedColumns().isEmpty()) continue;
			NestingStrategy adapted = null;
			String message = null;
			if (ns.getParentEdge().getName().matches(EdgeName.SUBQUERY)) {
				ExpressionPath ep = ExpressionPath.build(ns, in);
				EdgeTest locet = null;
				for(EdgeTest et : supportedLocations) {
					if (ep.has(et)) {
						locet = et;
						break;
					}
				}
				if (locet == null) {
					throw new PEException("Unable to classify subquery " + ns + " of statement " + in);
				}

				for(StrategyFactory sf : strategies) {
					adapted = sf.adapt(sc,locet, in, (Subquery)ns.getParent(), ep);
					if (adapted != null)
						break;
				}
				if (adapted == null)
					message = "No nesting handler for subquery ";
			}
			if (message == null && adapted == null)
				message = "No support for subquery ";
			if (message != null) 
				throw new PEException(message + ns.getSQL(sc) + " used in query " + in.getSQL(sc));
			else
				subs.add(adapted);
		}		
	}
	
	@Override
	public FeaturePlannerIdentifier getFeaturePlannerID() {
		return FeaturePlannerIdentifier.NESTED_QUERY;
	}

	@Override
	public FeatureStep plan(DMLStatement stmt, PlannerContext incontext)
			throws PEException {
		if (!applies(incontext.getContext(),stmt))
			return null;
		
		PlannerContext context = incontext.withTransform(getFeaturePlannerID());
		final SchemaContext sc = context.getContext();
		
		DMLStatement copy = (DMLStatement) CopyVisitor.copy(stmt);
		ListSet<NestingStrategy> handlers = new ListSet<NestingStrategy>();
		
		// change of plans, so to speak
		// we should:
		// [1] classify - but only do preplan rewrites at this point
		// [2] execute preplan rewrites - go back to [1]
		// [3] if no remaining rewrites plan with no further rewrites
		// [4] remaining rewrites - mod parent as necessary
		// [5] plan children as necessary
		// [6] plan parent if necessary
		// [7] fold in children if necessary
		// [8] replan parent if necessary
		
		boolean modified = false;
		do {
			modified = false;
			classifySubqueries(sc, copy, handlers);
			for(NestingStrategy ns : handlers) {
				String beforeHand = null, afterwards = null;
				if (emitting())
					beforeHand = copy.getSQL(sc);
				DMLStatement out = (DMLStatement) ns.beforeChildPlanning(sc,copy);
				if (emitting())
					afterwards = copy.getSQL(sc);
				if (out != null) {
					if (emitting()) {
						emit("pre  before child planning: " + beforeHand);
						emit("post before child planning: " + afterwards);
					}
					copy = out;
					copy.getBlock().clear();
					modified = true;
					break;
				}
			}
			if (modified == true)
				// run the whole analysis again
				handlers.clear();
		} while(modified == true);
		
		if (handlers.isEmpty()) {
			return buildPlan(copy,context,DefaultFeaturePlannerFilter.INSTANCE);
		}

		PlannerContext childContext = context.withCosting();
				
		// for everything that's left, plan it separately
		for(NestingStrategy ns : handlers) {
			FeatureStep fs = buildPlan(ns.getSubquery().getStatement(),childContext,
					new ComplexFeaturePlannerFilter(Collections.<FeaturePlannerIdentifier> emptySet(), Collections.singleton(getFeaturePlannerID())));
			ns.setStep(fs);
		}

		DMLStatement nss = CopyVisitor.copy(copy);
		if (emitting())
			emit("pre paste: " + nss.getSQL(sc));
		ListSet<FeatureStep> childSteps = new ListSet<FeatureStep>();
		for(NestingStrategy ns : handlers) {
			DMLStatement mod = ns.afterChildPlanning(context,nss, copy, this, childSteps);
			if (mod != null) nss = mod;
			if (emitting())
				emit("post paste: " + nss.getSQL(sc));
		}
		// now that we have an adjusted select statement, plan that as normal
		FeatureStep current = buildPlan(nss,context,DefaultFeaturePlannerFilter.INSTANCE);
		// we have to rewrite the children to add anything that the handlers did.
		
		for(NestingStrategy ns : handlers) {
			FeatureStep sub = ns.afterParentPlanning(context,current,this, childSteps);
			if (sub != null) current = sub;
		}
		current.prefixChildren(childSteps);
		
		return current;
	}
}
