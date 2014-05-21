// OS_STATUS: public
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


import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PESQLStateException;
import com.tesora.dve.sql.expression.ExpressionPath;
import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.Subquery;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.expression.TempTableInstance;
import com.tesora.dve.sql.node.structural.FromTableReference;
import com.tesora.dve.sql.node.test.EdgeTest;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.TempTable;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.DeleteStatement;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.statement.dml.UpdateStatement;
import com.tesora.dve.sql.transform.CopyContext;
import com.tesora.dve.sql.transform.CopyVisitor;
import com.tesora.dve.sql.transform.KeyCollector;
import com.tesora.dve.sql.transform.UniqueKeyCollector;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.featureplan.FeaturePlanner;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.ProjectingFeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.RedistFeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.RedistributionFlags;
import com.tesora.dve.sql.transform.strategy.nested.NestingStrategy.ScalarCheckResult;
import com.tesora.dve.sql.util.ListSet;

public class HandleScalarSubquery extends StrategyFactory {

	Boolean hasAllPK = null;
	
	@Override
	public NestingStrategy adapt(SchemaContext sc, EdgeTest location,
			DMLStatement enclosing, Subquery sq, ExpressionPath path)
			throws PEException {
		if ((EngineConstant.PROJECTION == location)
				|| (EngineConstant.UPDATECLAUSE == location)
				|| (EngineConstant.WHERECLAUSE == location)) {

			if (computeScalar(sc, sq.getStatement(), location)) {
				return new ScalarHandler(sq,path,location,hasAllPK);
			}
		}
		return null;
	}

	
	private boolean computeScalar(SchemaContext sc, ProjectingStatement nested, EdgeTest location) throws PESQLStateException {
		final ScalarCheckResult scalarCheck = NestingStrategy.hasScalarResult(sc, nested);
		if (scalarCheck.isValid()) {
			return true;
		} else if (!scalarCheck.isValid() && (scalarCheck != ScalarCheckResult.UNKNOWN)) {
			if ((scalarCheck == ScalarCheckResult.HAS_TOO_MANY_COLUMNS) && (EngineConstant.UPDATECLAUSE == location)) {
				throw new PESQLStateException(1241, "21000", "Operand should contain 1 column(s)");
			}

			return false;
		}

		final ListSet<TableKey> nestedTables = EngineConstant.TABLES_INC_NESTED.getValue(nested, sc);
        if (nestedTables.size() != 1){
			if (EngineConstant.UPDATECLAUSE == location) {
				throw new PESQLStateException(1052, "23000", "Column 'id' in field list is ambiguous");
			}

            return false;
        }

        hasAllPK = hasAllPK(sc, nested);
		if (Boolean.FALSE.equals(hasAllPK)
				&& ((EngineConstant.WHERECLAUSE == location)
				|| (EngineConstant.UPDATECLAUSE == location))) {
        	// we can relax the unique check when the uncorrelated subquery is in the where clause
			// also we assume that a subquery in the update expression returns a
			// single value
        	return true;
        }
        
        return hasAllPK;
	}
	
	private boolean hasAllPK(SchemaContext sc, ProjectingStatement nested) {
        //ok, now we know we only have one table in the subquery, so check if all its pk is present.

        UniqueKeyCollector uniqueCheck = new UniqueKeyCollector(sc,nested);
        ListSet<KeyCollector.Part> pkParts = uniqueCheck.getParts();
        if (pkParts.isEmpty()){
            return false;
        }

        boolean haveAllParts = true;
        for (KeyCollector.Part part : pkParts){
            if (!part.isComplete()){
                haveAllParts = false;
                break;
            }
        }

        return haveAllParts;
    }

	public HandleScalarSubquery() {
		
	}

	protected static class ScalarHandler extends NestingStrategy {

		EdgeTest location;
		ExpressionNode expr;
		ExpressionPath within;
		int offset;
		Boolean hasPK = null;
		
		public ScalarHandler(Subquery nested, ExpressionPath pathTo, EdgeTest location, Boolean hasPK) {
			super(nested, pathTo);
			this.location = location;
			this.hasPK = hasPK;
		}

		@Override
		public DMLStatement beforeChildPlanning(SchemaContext sc, DMLStatement orig) throws PEException {
			if (location != EngineConstant.PROJECTION) {
				return null;
			}
			SelectStatement ss = (SelectStatement) orig;
			offset = -1;
			expr = null;
			int counter = -1;
			for(Iterator<ExpressionNode> iter = ss.getProjectionEdge().iterator(); iter.hasNext();) {
				counter++;
				ExpressionNode en = iter.next();
				if (sq.getStatement().ifAncestor(Collections.singleton(en)) != null) {
					offset = counter;
					expr = en;
					break;
				}
			}
			if (offset == -1)
				throw new PEException("Cannot find subquery within projection");
			within = ExpressionPath.build(sq, expr);
			orig.getDerivedInfo().getLocalNestedQueries().remove(sq.getStatement());
			return null;
		}

		@Override
		public DMLStatement afterChildPlanning(PlannerContext pc, DMLStatement orig, DMLStatement preRewrites, FeaturePlanner planner, List<FeatureStep> collector) throws PEException {
			if ((preRewrites instanceof UpdateStatement)
					|| (preRewrites instanceof DeleteStatement)) {
				// Scalar subqueries should already be collocated from
				// NestedQueryBroadcastTransformFactory applied on UPDATE and
				// DELETE statements.
				return orig;
			}
				
			SelectStatement ss = (SelectStatement) orig;
			if (location == EngineConstant.PROJECTION) {
				SelectStatement beforeRewrites = (SelectStatement) preRewrites;
				int delta = beforeRewrites.getProjectionEdge().size() - ss.getProjectionEdge().size();
				// now remove
				List<ExpressionNode> proj = new ArrayList<ExpressionNode>(ss.getProjection());
				proj.remove(offset - delta);
				ss.setProjection(proj);
				return ss;
			} else {
				RedistFeatureStep rfs = 
						buildChildBCastTempTableStep(pc, (ProjectingFeatureStep)planned,ss.getStorageGroups(pc.getContext()),planner,
								new RedistributionFlags().withEnforceScalarValue());
				collector.add(rfs);
				setStep(rfs);
				TempTable ct = rfs.getTargetTempTable();
				// now - we originally had a subquery; now we're going to build a new table instance from
				// the temp table and replace the subquery with the table instance
				TableInstance ti = new TempTableInstance(pc.getContext(),ct);
				// and we need a column instance for the scalar value
				ColumnInstance ci = new ColumnInstance(ct.getColumns(pc.getContext()).get(0),ti);
				SelectStatement origChild = (SelectStatement) pathWithinEnclosing.apply(orig);
				// the parent is a subquery node, so we want the parent of the parent
				Subquery sq = (Subquery) origChild.getParent();
				sq.getParentEdge().set(ci);
				// also add the new table to the from clause
				ss.getTablesEdge().add(new FromTableReference(ti));
				orig.getDerivedInfo().getLocalNestedQueries().remove(origChild);
				orig.getDerivedInfo().addLocalTable(ti.getTableKey());
				return orig;
			}
		}

		@Override
		public FeatureStep afterParentPlanning(PlannerContext pc, FeatureStep parentStep, FeaturePlanner planner,
				List<FeatureStep> collector) throws PEException {

			SelectStatement stmt = (SelectStatement)planned.getPlannedStatement();
			if (NestingStrategy.hasLimitOne(pc.getContext(), stmt))
				parentStep.withCachingFlag(false);

			if (location != EngineConstant.PROJECTION)
				return null;

            RedistFeatureStep rfs = 
            		buildChildBCastTempTableStep(pc,(ProjectingFeatureStep)planned,Collections.singletonList(parentStep.getSourceGroup()),
            				planner,(RedistributionFlags)null);

            collector.add(rfs);
            
            TempTable ct = rfs.getTargetTempTable();            
			// now - we originally had a subquery; now we're going to build a new table instance from
			// the temp table and replace the subquery with the table instance
			
			TableInstance ti = new TempTableInstance(pc.getContext(), ct);
			ColumnInstance ci = new ColumnInstance(ct.getColumns(pc.getContext()).get(0),ti);

			SelectStatement ss = (SelectStatement) parentStep.getPlannedStatement();
			List<ExpressionNode> proj = new ArrayList<ExpressionNode>(ss.getProjection());
			ExpressionNode projEntryCopy = 
					CopyVisitor.copy(expr, new CopyContext("HandleProjectionScalarSubquery.afterParentPlanning")); 
			within.update(projEntryCopy, ci);			
			proj.add(offset, projEntryCopy);
			ss.setProjection(proj);
			// renormalize
			ss.normalize(pc.getContext());

			ss.getTablesEdge().add(new FromTableReference(ti));
			ss.getDerivedInfo().addLocalTable(ti.getTableKey());
			
			return parentStep;
		}
	}
		
}
