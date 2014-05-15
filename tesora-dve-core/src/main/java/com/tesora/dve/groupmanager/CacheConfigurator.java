// OS_STATUS: public
package com.tesora.dve.groupmanager;

public interface CacheConfigurator {
	
	interface Factory {
		public CacheConfigurator newInstance();
	}

	void shutdown();
}
