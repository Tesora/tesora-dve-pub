// OS_STATUS: public
package com.tesora.dve.common.catalog;

public interface IDynamicPolicy {

	public DynamicGroupClass getAggregationClass();

	public DynamicGroupClass getSmallClass();

	public DynamicGroupClass getMediumClass();

	public DynamicGroupClass getLargeClass();

	public boolean getStrict();

}
