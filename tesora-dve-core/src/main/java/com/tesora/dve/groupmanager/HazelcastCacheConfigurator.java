// OS_STATUS: public
package com.tesora.dve.groupmanager;


public class HazelcastCacheConfigurator implements CacheConfigurator {
	
	static class Factory implements CacheConfigurator.Factory {

		@Override
		public CacheConfigurator newInstance() {
			return new HazelcastCacheConfigurator();
		}
		
	}
	
	protected static final String TYPE = "com.hazelcast.hibernate.HazelcastCacheRegionFactory";

	@Override
	public void shutdown() {
	}
}
