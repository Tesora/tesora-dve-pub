// OS_STATUS: public
package com.tesora.dve.groupmanager;

public class DefaultCacheConfigurator implements CacheConfigurator {
	
	static class Factory implements CacheConfigurator.Factory {
		@Override
		public CacheConfigurator newInstance() {
			return new DefaultCacheConfigurator();
		}
	}
	
	static final String TYPE = "Default";

	@Override
	public void shutdown() {
	}

}
