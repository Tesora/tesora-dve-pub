// OS_STATUS: public
package com.tesora.dve.sql.transform.strategy.featureplan;

import com.tesora.dve.sql.node.AbstractTraversal;

public abstract class StepTraversal extends AbstractTraversal<FeatureStep> {


	public StepTraversal(Order direction, ExecStyle execStyle) {
		super(direction, execStyle);
	}

	@Override
	protected void traverseInternal(FeatureStep n) {
		for(FeatureStep fs : n.getAllChildren())
			if (allow(fs))
				traverse(fs);
	}

}
