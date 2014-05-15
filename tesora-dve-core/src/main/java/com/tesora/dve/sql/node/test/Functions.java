// OS_STATUS: public
package com.tesora.dve.sql.node.test;


import com.tesora.dve.sql.node.AbstractTraversal.Order;
import com.tesora.dve.sql.node.CollectingTraversal;
import com.tesora.dve.sql.node.DerivedAttribute;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.util.ListSet;

public class Functions extends DerivedAttribute<ListSet<FunctionCall>> {

	@Override
	public boolean isApplicableSubject(LanguageNode ln) {
		return (ln instanceof DMLStatement);
	}

	@Override
	public ListSet<FunctionCall> computeValue(SchemaContext sc, LanguageNode ln) {
		return CollectingTraversal.collect(ln, new CollectingTraversal(Order.POSTORDER,EngineConstant.FUNCTION));
	}

}
