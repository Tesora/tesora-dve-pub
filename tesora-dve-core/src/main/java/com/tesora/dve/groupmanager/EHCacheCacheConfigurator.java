// OS_STATUS: public
package com.tesora.dve.groupmanager;

public class EHCacheCacheConfigurator implements CacheConfigurator {
	
	static class Factory implements CacheConfigurator.Factory {

		@Override
		public CacheConfigurator newInstance() {
			return new EHCacheCacheConfigurator();
		}
		
	}

	protected static final String TYPE1 = "net.sf.ehcache.hibernate.SingletonEhCacheRegionFactory";
	protected static final String TYPE2 = "org.hibernate.cache.ehcache.SingletonEhCacheRegionFactory";

	@Override
	public void shutdown() {
	}
}
