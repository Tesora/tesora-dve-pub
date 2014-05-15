// OS_STATUS: public
package com.tesora.dve.server.bootstrap;

public interface BootstrapHostMBean {
	
	int getPortalWorkerGroupCount();
	
	int getPortalClientExecutorActiveCount();

	int getPortalClientExecutorLargestPoolSize();

	int getPortalClientExecutorMaximumPoolSize();

	int getPortalClientExecutorQueueSize();

	int getPortalClientExecutorPoolSize();
}
