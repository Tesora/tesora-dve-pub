// OS_STATUS: public
package com.tesora.dve.sql.transform;


import java.util.Arrays;

import com.tesora.dve.sql.node.GeneralCollectingTraversal;
import com.tesora.dve.sql.node.LanguageNode;
import com.tesora.dve.sql.node.expression.ExpressionNode;
import com.tesora.dve.sql.node.expression.TableInstance;
import com.tesora.dve.sql.node.test.EngineConstant;
import com.tesora.dve.sql.statement.dml.DMLStatement;
import com.tesora.dve.sql.util.ListSet;

public final class TableInstanceCollector extends GeneralCollectingTraversal {

	private TableInstanceCollector() {
		super(Order.POSTORDER, ExecStyle.ONCE);
	}

	public static ListSet<TableInstance> getInstances(DMLStatement... dmls) {
		return GeneralCollectingTraversal.collect(Arrays.asList(dmls), new TableInstanceCollector());
	}

	public static ListSet<TableInstance> getInstances(ExpressionNode en) {
		return GeneralCollectingTraversal.collect(en, new TableInstanceCollector());
	}
	
	@Override
	public boolean is(LanguageNode ln) {
		return EngineConstant.TABLE.has(ln);
	}	
}
