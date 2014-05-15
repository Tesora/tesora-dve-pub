// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.statement.dml.SelectStatement;
import com.tesora.dve.sql.transform.behaviors.DefaultFeaturePlannerFilter;
import com.tesora.dve.sql.transform.behaviors.FeaturePlannerFilter;
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
		for(TransformFactory t : statement.getTransformers()) {
			if (!filter.canApply(context, t.getFeaturePlannerID())) continue;
			out = t.plan(statement, context);
			if (out != null)
				break;
		}
		if (out != null)
			out.maintainInvariants(context);
		return out;
	}

	public static void featurePlan(SchemaContext sc, DMLStatement stmt, ExecutionSequence sequence) throws PEException {
		PlannerContext pc = new PlannerContext(sc);
		FeatureStep fp = buildPlan(stmt,pc,DefaultFeaturePlannerFilter.INSTANCE);
		if (fp == null)
			throw new PEException("No applicable planning for '" + stmt.getSQL(sc) + "'");
		pc.getTempGroupManager().plan(sc);
		maintainInvariants(sc,stmt,fp.getPlannedStatement());
		fp.schedule(pc, sequence, new HashSet<FeatureStep>());
	}

}
