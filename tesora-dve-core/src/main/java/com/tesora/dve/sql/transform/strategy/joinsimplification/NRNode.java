// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.joinsimplification;

import java.util.Set;

import com.tesora.dve.sql.expression.TableKey;
import com.tesora.dve.sql.node.expression.ExpressionNode;

public abstract class NRNode {

	protected NRNode parent;
	protected ExpressionNode wrapping;
	
	protected Set<TableKey> uses;
	
	public NRNode(ExpressionNode en) {
		wrapping = en;
		parent = null;
		uses = null;
	}
	
	public abstract boolean required(TableKey tab);

	protected abstract Set<TableKey> computeUses();
	
	public final Set<TableKey> uses() {
		if (uses == null) uses = computeUses();
		return uses;
	}
}
