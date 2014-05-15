// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.joinsimplification;

import java.util.Collections;
import java.util.Set;

import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.ColumnInstance;
import com.tesora.dve.sql.node.expression.ExpressionNode;

// either a constant or a column reference
public class SimpleNode extends NRNode {

	public SimpleNode(ExpressionNode en) {
		super(en);
	}

	@Override
	public boolean required(TableKey tab) {
		if (wrapping instanceof ColumnInstance) {
			ColumnInstance ci = (ColumnInstance) wrapping;
			return (ci.getTableInstance().getTableKey().equals(tab));
		}
		// otherwise a constant
		return false;
	}

	@Override
	protected Set<TableKey> computeUses() {
		if (wrapping instanceof ColumnInstance) {
			ColumnInstance ci = (ColumnInstance) wrapping;
			return Collections.singleton(ci.getTableInstance().getTableKey());
		}
		return Collections.emptySet();
	}

}
