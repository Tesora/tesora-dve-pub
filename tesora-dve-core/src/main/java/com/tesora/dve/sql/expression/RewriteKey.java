// OS_STATUS: public
package com.tesora.dve.sql.expression;

import com.tesora.dve.sql.node.LanguageNode;

public abstract class RewriteKey {

	protected Integer hc = null;
	
	protected RewriteKey() {
	}
	
	@Override
	public int hashCode() {
		if (hc == null)
			hc = computeHashCode();
		return hc;
	}

	protected abstract int computeHashCode();
	
	public abstract LanguageNode toInstance();
	
	public void clearHashCode() {
		hc = null;
	}
	
}
