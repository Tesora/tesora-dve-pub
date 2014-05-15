// OS_STATUS: public
package com.tesora.dve.queryplan;

public abstract class QueryStepModifyTimestampVariableOperation extends
		QueryStepOperation {

	long timestampVariableValue = 0;

	public long getTimestampVariableValue() {
		return timestampVariableValue;
	}

	public void setTimestampVariableValue(long timestampVariableValue) {
		this.timestampVariableValue = timestampVariableValue;
	}
	
}
