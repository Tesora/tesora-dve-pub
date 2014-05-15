// OS_STATUS: public
package com.tesora.dve.worker;

import com.tesora.dve.common.catalog.DynamicPolicy;
import com.tesora.dve.common.catalog.StorageGroup;
import com.tesora.dve.exceptions.PEException;

public class AggregationGroup extends DynamicGroup {
	
	private static final long serialVersionUID = 1L;

	public AggregationGroup(DynamicPolicy template) throws PEException {
		super(template, StorageGroup.GroupScale.AGGREGATE);
	}
}
