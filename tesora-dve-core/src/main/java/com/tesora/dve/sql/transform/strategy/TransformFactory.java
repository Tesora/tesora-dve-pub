package com.tesora.dve.sql.transform.strategy;

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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.behaviors.BehaviorConfiguration;
import com.tesora.dve.sql.transform.behaviors.FeaturePlannerFilter;
import com.tesora.dve.sql.transform.behaviors.defaults.DefaultFeaturePlannerFilter;
import com.tesora.dve.sql.transform.execution.ExecutionSequence;
import com.tesora.dve.sql.transform.strategy.featureplan.FeaturePlanner;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;

public abstract class TransformFactory implements FeaturePlanner {

	public static final Set<FeaturePlannerIdentifier> allTransforms = 
			new HashSet<FeaturePlannerIdentifier>(Arrays.asList(FeaturePlannerIdentifier.values()));
	
	public abstract FeaturePlannerIdentifier getFeaturePlannerID();
	
	protected static void maintainInvariants(SchemaContext sc, DMLStatement orig, DMLStatement result) throws PEException {
		if (orig instanceof SelectStatement) {
			SelectStatement os = (SelectStatement) orig;
			int origProj = os.getProjection().size();
			if (result instanceof SelectStatement) {
				SelectStatement rs = (SelectStatement) result;
				int finProj = rs.getProjection().size();
				if (origProj < finProj)
					throw new PEException("Internal error: after planning projection has too many columns");
				else if (origProj > finProj) 
					throw new PEException("Internal error: after planning projection has too few columns.  Started with " + orig.getSQL(sc) + " but ended with " + rs.getSQL(sc));
				int mdlength = os.getProjectionMetadata(sc).getWidth();
				if (finProj < mdlength) 
					throw new PEException("Internal error: after planning projection is smaller than expected metadata length");
				else if (finProj > mdlength)
					throw new PEException("Internal error: after planning projection is larger than expected metadata length");
			}
		}
	}	
	
	protected final boolean emitState = Boolean.getBoolean(getFeaturePlannerID().toString());

	public void emit(String what) {
		if (emitState) {
			System.out.println(getFeaturePlannerID().toString() + ": " + what);
			System.out.println();
		}
	}

	public boolean emitting() {
		return emitState;
	}

	public static FeatureStep buildPlan(DMLStatement statement, PlannerContext context, FeaturePlannerFilter filter) throws PEException {
		statement.normalize(context.getContext());
		FeatureStep out = null;
		for(FeaturePlanner fp : context.getBehaviorConfiguration().getFeaturePlanners(context, statement)) {
			if (!filter.canApply(context, fp.getFeaturePlannerID())) continue;
			out = fp.plan(statement, context);
			if (out != null)
				break;
		}
		if (out != null)
			out.maintainInvariants(context);
		return out;
	}

	public static void featurePlan(SchemaContext sc, DMLStatement stmt, ExecutionSequence sequence, BehaviorConfiguration config) throws PEException {
		PlannerContext pc = new PlannerContext(sc,config);
		FeatureStep fp = buildFeatureStep(pc,stmt);
		fp.schedule(pc, sequence, new HashSet<FeatureStep>());
	}
	
	public static FeatureStep buildFeatureStep(PlannerContext pc, DMLStatement stmt) throws PEException {
		FeatureStep fp = buildPlan(stmt,pc,DefaultFeaturePlannerFilter.INSTANCE);
		if (fp == null)
			throw new PEException("No applicable planning for '" + stmt.getSQL(pc.getContext()) + "'");
		fp = pc.getBehaviorConfiguration().getPostPlanningTransformer(pc, stmt).transform(pc, stmt, fp);
		pc.getTempGroupManager().plan(pc.getContext());
		maintainInvariants(pc.getContext(),stmt,fp.getPlannedStatement());
		return fp;
	}
	
}
