// OS_STATUS: public
package com.tesora.dve.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.log4j.Logger;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.tesora.dve.common.PEUrl;
import com.tesora.dve.exceptions.PEException;

/**
 * This class manages a list of URLs that are provided via the initialize() method. The getUrl() method
 * is used to retrieve a URL from the. As currently implemented, it will return one randomly from the
 * list. If the caller decides the URL has "failed", a call to failUrl() is made. This will put the URL
 * into a failed list. The failed list has an expiry elapsed time (also provided to initialize()) that will
 * cause the URLs to be evicted from the fail list after the time has passed. After eviction, they will
 * be available for the getUrl() call to use.
 *  
 */
public final class UrlBalancer{
	static final Logger logger = Logger.getLogger(UrlBalancer.class);

	private List<PEUrl> managedUrlList  = Collections.synchronizedList(new ArrayList<PEUrl>());
	private Cache<String, PEUrl> failedUrlCache;
	
	private static final UrlBalancer INSTANCE = new UrlBalancer();
	
	private UrlBalancer() {}
	
	/**
	 * Initializes the singleton instance of this class. Any previous state will be cleared.
	 *  
	 * @param failedUrlExpiration - int - number of seconds to elapse after failUrl() is called before
	 * 				the Url is available to getUrl() again
	 * @param providedUrlList - List<String> - a List containing the string representation of valid URLs
	 * @throws PEException
	 * 			- if the providedUrlList is empty
	 * 			- if any of the URLs in the list are invalid
	 */
	public void initialize(int failedUrlExpiration, List<String> providedUrlList) throws PEException {
		synchronized (managedUrlList) {
			if ( logger.isDebugEnabled() )
				logger.debug("Initializing URL Balancer with failed URL expiry of " + failedUrlExpiration);

			close();
			failedUrlCache = CacheBuilder.newBuilder()
					.expireAfterWrite(failedUrlExpiration, TimeUnit.SECONDS)
					.build();
			
			for (String urlStr : providedUrlList) {
				managedUrlList.add(PEUrl.fromUrlString(urlStr));
				if ( logger.isDebugEnabled() )
					logger.debug("URL Balancer initialized with '" + urlStr + "'");
			}

			if (managedUrlList.isEmpty())
				throw new PEException(
						"No addresses were added to the Active URL List from provided list");
		}
	}
	
	/**
	 * Clears all state held within the UrlBalancer
	 */
	public void close() {
		synchronized (managedUrlList) {
			managedUrlList.clear();
			if (failedUrlCache != null) {
				failedUrlCache.invalidateAll();
				failedUrlCache = null;
			}
		}
	}

	/**
	 * Returns a single URL from the list. Will not return any URLs that are currently failed.
	 * 
	 * @return - PEUrl - a random URL from the list
	 * @throws PEException
	 * 			- URL list is empty
	 * 			- all the URLs in the list are failed
	 */
	public PEUrl getUrl() throws PEException {
		if (managedUrlList.isEmpty())
			throw new PEException("Managed URL List doesn't contain any entries");

		PEUrl urlCandidate = null;
		boolean urlNotFound = true;
		while ( urlNotFound ) {
			urlCandidate = managedUrlList.get(RandomUtils.nextInt(managedUrlList.size()));
			if ( failedUrlCache.getIfPresent(urlCandidate.toString()) == null ) {
				urlNotFound = false;
			}
			if (managedUrlList.size() == failedUrlCache.size() )
				throw new PEException("All URLs in managed list have been marked as failed");
		}
		
		if ( logger.isDebugEnabled() )
			logger.debug("URL Balancer returning URL " + urlCandidate.getURL());
		
		return urlCandidate;
	}

	/**
	 * Returns the current size of the URL list. Will include any URLs that were failed.
	 * 
	 * @return int
	 */
	public int size() {
		return managedUrlList.size();
	}

	/**
	 * Used to mark urlToFail as currently being failed. Once failed this URL will not be used by 
	 * the getURL() call again until the failedUrlExpiration time has passed.
	 * 
	 * @param urlToFail - PEUrl - the URL to mark as failed
	 * @throws PEException
	 * 				- if urlToFail isn't in the list
	 */
	public void failUrl(PEUrl urlToFail) throws PEException {
		if ( !managedUrlList.contains(urlToFail) )
			throw new PEException("The URL marked to fail isn't in the managed list (" + urlToFail + ")");

		failedUrlCache.put(urlToFail.toString(), urlToFail);
		
		if ( logger.isDebugEnabled() )
			logger.debug("URL Balancer is tracking '" + urlToFail + "' as failed");
	}

	/**
	 * Return the singleton instance of the UrlBalancer
	 * 
	 * @return - UrlBalancer
	 */
	public static UrlBalancer getInstance() {
		return INSTANCE;
	}
}
