// OS_STATUS: public
package com.tesora.dve.sql.node.test;

import com.tesora.dve.sql.node.AbstractTraversal.ExecStyle;
import com.tesora.dve.sql.node.AbstractTraversal.Order;
import com.tesora.dve.sql.node.DerivedAttribute;
import com.tesora.dve.sql.node.GeneralCollectingTraversal;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.FunctionCall;
import com.tesora.dve.sql.schema.SchemaContext;
import com.tesora.dve.sql.util.ListSet;

public class Equijoins extends DerivedAttribute<ListSet<FunctionCall>> {

	@Override
	public boolean isApplicableSubject(LanguageNode ln) {
		return true;
	}

	@Override
	public ListSet<FunctionCall> computeValue(final SchemaContext sc, LanguageNode ln) {
		return GeneralCollectingTraversal.collect(ln, new GeneralCollectingTraversal(Order.PREORDER,ExecStyle.ONCE) {

			@Override
			public boolean is(LanguageNode ln) {
				Boolean value = ln.getDerivedAttribute(EngineConstant.EQUIJOIN, sc);
				if (value == null) return false;
				return value.booleanValue();
			}
		});
	}

}
