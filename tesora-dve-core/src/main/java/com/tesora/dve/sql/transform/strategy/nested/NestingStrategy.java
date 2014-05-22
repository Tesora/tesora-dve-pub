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



import java.util.List;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.expression.ExpressionPath;
import com.tesora.dve.sql.expression.ExpressionUtils;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.node.expression.Subquery;
import com.tesora.dve.sql.node.structural.LimitSpecification;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.schema.DistributionVector.Model;
import com.tesora.dve.sql.schema.PEStorageGroup;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.schema.TempTableCreateOptions;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.ProjectingStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.statement.dml.UnionStatement;
import com.tesora.dve.sql.transform.AggFunCollector;
import com.tesora.dve.sql.transform.ColumnInstanceCollector;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.util.ListSet;
import com.tesora.dve.sql.transform.strategy.featureplan.FeaturePlanner;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.ProjectingFeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.RedistFeatureStep;
import com.tesora.dve.sql.transform.strategy.featureplan.RedistributionFlags;

public abstract class NestingStrategy {
	
	public static enum ScalarCheckResult {
		IN_EXISTS_FCN(true, "The query is called within an exists function which returns a scalar."),
		IS_UNION_STATEMENT(false, "The statement is a union."),
		HAS_TOO_MANY_COLUMNS(false, "The query returns more than one column."),
		IN_AGGREGATION_FCN(true, "The query returns a single column in an aggregation function, without a group by clause."),
		IS_LITERAL(true, "The query returns a single literal expression."),
		HAS_LIMIT_ONE(true, "The query returns a single column and has a limit of one."),
		UNKNOWN(false, "We cannot prove this query has a scalar result.");

		private final boolean valid;
		private final String describtion;

		private ScalarCheckResult(final boolean valid, final String describtion) {
			this.valid = valid;
			this.describtion = describtion;
		}

		public boolean isValid() {
			return this.valid;
		}

		public String getDescribtion() {
			return this.describtion;
		}
	}

	protected ExpressionPath pathWithinEnclosing;
	protected Subquery sq;
	protected ProjectingStatement nested;

	protected FeatureStep planned;

	public NestingStrategy(Subquery nested, ExpressionPath pathTo) {
		this.sq = nested;
		this.pathWithinEnclosing = pathTo;
		this.nested = sq.getStatement();
	}
	

	public Subquery getSubquery() {
		return sq;
	}
	
	public ExpressionPath getPathWithinEnclosing() {
		return pathWithinEnclosing;
	}
		
	public void setStep(FeatureStep planned) {
		this.planned = planned;
	}
	
	public FeatureStep getStep() {
		return this.planned;
	}
	
	public DMLStatement beforeChildPlanning(SchemaContext sc, DMLStatement parent) throws PEException {
		return null;
	}
	
	public DMLStatement afterChildPlanning(PlannerContext pc, DMLStatement parent, DMLStatement preREwrites, FeaturePlanner planner, List<FeatureStep> childSteps) throws PEException {
		return null;
	}
	
	public FeatureStep afterParentPlanning(PlannerContext pc, FeatureStep parentStep, FeaturePlanner planner, List<FeatureStep> childSteps) throws PEException {
		return null;
	}
	
	public static ScalarCheckResult hasScalarResult(SchemaContext sc, ProjectingStatement ps) {		
		// if the statement is called within an exists function - we don't actually require a scalar result.
		if (EngineConstant.FUNCTION.has(ps.getParent().getParent(), EngineConstant.EXISTS)) {
			return ScalarCheckResult.IN_EXISTS_FCN;
		}

		if (ps instanceof UnionStatement) {
			return ScalarCheckResult.IS_UNION_STATEMENT;
		}

		final SelectStatement ss = (SelectStatement) ps;
		// a scalar query is one which returns a single value - so it has one column, and that column will have one value
		if (ss.getProjectionEdge().size() > 1) {
			return ScalarCheckResult.HAS_TOO_MANY_COLUMNS;
		}

		final ExpressionNode singleColumn = ExpressionUtils.getTarget(ss.getProjectionEdge().get(0));
		final ListSet<FunctionCall> anyAggs = AggFunCollector.collectAggFuns(singleColumn);
		if (!anyAggs.isEmpty() && !EngineConstant.GROUPBY.has(ss)) {
			// single column aggr, without a group by - must be a grand agg
			// (i.e. max(id))
			return ScalarCheckResult.IN_AGGREGATION_FCN;
		}
		// could be a literal, check for that too
		ListSet<ColumnInstance> anyColumns = ColumnInstanceCollector.getColumnInstances(singleColumn);
		if (anyColumns.isEmpty()) {
			return ScalarCheckResult.IS_LITERAL;
		}
		
		if (hasLimitOne(sc, ss)) {
			return ScalarCheckResult.HAS_LIMIT_ONE;
		}
	
		return ScalarCheckResult.UNKNOWN;
	}
	
	public static boolean hasLimitOne(final SchemaContext sc, final SelectStatement select) {
		final LimitSpecification limitSpec = select.getLimit();
		return ((limitSpec != null) && limitSpec.hasLimitOne(sc));
	}

	protected RedistFeatureStep buildChildBCastTempTableStep(PlannerContext pc, ProjectingFeatureStep pfs, 
			List<PEStorageGroup> enclosingGroups, FeaturePlanner planner,
			RedistributionFlags flags)
		throws PEException {
		PEStorageGroup targetGroup = null;
		if (enclosingGroups.size() == 1)
			targetGroup = enclosingGroups.get(0);
		else
			targetGroup = pc.getTempGroupManager().getGroup(pfs.getCost().getGroupScore());
		return pfs.redist(pc, planner,
				new TempTableCreateOptions(Model.BROADCAST,targetGroup)
					.withRowCount(pfs.getCost().getRowCount()),
					flags,
					null);
	}

}
