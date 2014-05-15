// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.joinsimplification;


import java.util.Collections;
import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.transform.CopyVisitor;
import com.tesora.dve.sql.transform.behaviors.ComplexFeaturePlannerFilter;
import com.tesora.dve.sql.transform.strategy.PlannerContext;
import com.tesora.dve.sql.transform.strategy.TransformFactory;
import com.tesora.dve.sql.transform.strategy.FeaturePlannerIdentifier;
import com.tesora.dve.sql.transform.strategy.featureplan.FeatureStep;

/*
 * Various simplifications around joins.  Does not do any join planning, just
 * simplifies the query.
 */
public class JoinSimplificationTransformFactory extends TransformFactory {

	private static final Simplifier[] simplifiers = new Simplifier[] {
		new MultijoinSimplifier(),
		new NullRejectionSimplifier(),
		new ConstantFoldingSimplifier()
	};
	
	private boolean applies(SchemaContext sc, DMLStatement stmt)
			throws PEException {
		for(Simplifier s : simplifiers) {
			if (s.applies(sc, stmt))
				return true;
		}
		return false;
	}

	@Override
	public FeaturePlannerIdentifier getFeaturePlannerID() {
		return FeaturePlannerIdentifier.JOIN_SIMPLIFICATION;
	}

	@Override
	public FeatureStep plan(DMLStatement stmt, PlannerContext ipc)
			throws PEException {
		if (!applies(ipc.getContext(), stmt))
			return null;
		
		PlannerContext context = ipc.withTransform(getFeaturePlannerID());
		DMLStatement copy = CopyVisitor.copy(stmt);

		boolean any = false;
		DMLStatement current = copy;
		for(Simplifier s : simplifiers) {
			DMLStatement mods = s.simplify(context.getContext(), current,this);
			if (mods != null) {
				current = mods;
				any = true;
			}
		}
		
		return buildPlan((any ? current : stmt), context.withTransform(getFeaturePlannerID()),
				new ComplexFeaturePlannerFilter(Collections.singleton(getFeaturePlannerID()), Collections.<FeaturePlannerIdentifier> emptySet()));
	}

	
}
