// OS_STATUS: public
package com.tesora.dve.sql.node.test;


import com.tesora.dve.sql.node.DerivedAttribute;
import com.tesora.dve.sql.node.Edge;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.transform.AggFunCollector;
import com.tesora.dve.sql.util.ListSet;

class ProjectionAggregateFunctions extends DerivedAttribute<ListSet<FunctionCall>> {

	@Override
	public boolean isApplicableSubject(LanguageNode ln) {
		return EngineConstant.PROJECTION.has(ln);
	}

	@Override
	public ListSet<FunctionCall> computeValue(SchemaContext sc, LanguageNode ln) {
		Edge<?,?> proj = EngineConstant.PROJECTION.getEdge(ln);
		return AggFunCollector.collectAggFuns(proj);
	}
	

}
