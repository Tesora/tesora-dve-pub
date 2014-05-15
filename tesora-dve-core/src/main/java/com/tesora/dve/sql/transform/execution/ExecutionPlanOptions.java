// OS_STATUS: public
package com.tesora.dve.sql.transform.execution;

import com.tesora.dve.sql.transform.execution.LateBindingUpdateCountFilter.RuntimeUpdateCounter;

public class ExecutionPlanOptions {

	private RuntimeUpdateCounter runtimeUpdateCounter = null;

	public void setRuntimeUpdateCounter(final RuntimeUpdateCounter counter) {
		this.runtimeUpdateCounter = counter;
	}
	
	public RuntimeUpdateCounter getRuntimeUpdateCounter() {
		return this.runtimeUpdateCounter;
	}
}
