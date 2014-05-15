// OS_STATUS: public
package com.tesora.dve.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

import com.tesora.dve.common.PEUrl;
import com.tesora.dve.common.UrlBalancer;
import com.tesora.dve.exceptions.PEException;

public class UrlBalancerTest {
	static final int FAILED_SITE_EXPIRY = 10;
	static final List<String> URL_LIST = new ArrayList<String>() {
		private static final long serialVersionUID = 1L;
		{
			add("jdbc:mysql://host1:1111");
			add("jdbc:mysql://host2:2222");
			add("jdbc:mysql://host3:3333");
			add("jdbc:mysql://host4:4444");
			add("jdbc:mysql://host5:5555");
			add("jdbc:mysql://host6:6666");
			add("jdbc:mysql://host7:7777");
			add("jdbc:mysql://host8:8888");
			add("jdbc:mysql://host9:9999");
			add("jdbc:mysql://host10:1010");
		}
	};
	
	Set<String> checkList = new HashSet<String>();

	@Test
	public void basicTest() throws Exception {
		UrlBalancer.getInstance().initialize(FAILED_SITE_EXPIRY, URL_LIST);
		checkList.clear();
		checkList.addAll(URL_LIST);
		
		assertEquals(10, UrlBalancer.getInstance().size());

		int i = 1;
		while ( !checkList.isEmpty() ) {
			checkList.remove(UrlBalancer.getInstance().getUrl().getURL());
			i++;
			assertTrue(i<100);
		}
		
		UrlBalancer.getInstance().close();
	}

	@Test
	public void failUrlTest() throws Exception {
		UrlBalancer.getInstance().initialize(FAILED_SITE_EXPIRY, URL_LIST);
		checkList.clear();
		checkList.addAll(URL_LIST);
		
		PEUrl failedUrl = UrlBalancer.getInstance().getUrl();
		UrlBalancer.getInstance().failUrl(failedUrl);
		checkList.remove(failedUrl.getURL());

		int i = 1;
		while ( !checkList.isEmpty() ) {
			assertFalse(failedUrl.equals(UrlBalancer.getInstance().getUrl()));
			checkList.remove(UrlBalancer.getInstance().getUrl().getURL());
			i++;
			assertTrue(i<100);
		}
		UrlBalancer.getInstance().close();
	}

	@Test (expected=PEException.class)
	public void failBadUrlTest() throws Exception {
		UrlBalancer.getInstance().initialize(FAILED_SITE_EXPIRY, URL_LIST);
		UrlBalancer.getInstance().failUrl(new PEUrl().createMysqlDefaultURL());
		UrlBalancer.getInstance().close();
	}
	
	@Test (expected=PEException.class)
	public void allFailedTest() throws Exception {
		UrlBalancer.getInstance().initialize(FAILED_SITE_EXPIRY, URL_LIST);
		
		for ( String urlStr : URL_LIST ) {
			UrlBalancer.getInstance().failUrl(PEUrl.fromUrlString(urlStr));
		}
		
		UrlBalancer.getInstance().getUrl();
		UrlBalancer.getInstance().close();
	}
	
	@Test
	public void failRecoveryTest() throws Exception {
		PEUrl failedURL = PEUrl.fromUrlString("jdbc:mysql://host1:1111");
		List<String> urlList = new ArrayList<String>();
		urlList.add(failedURL.getURL());
		UrlBalancer.getInstance().initialize(1, urlList);
		
		assertTrue(failedURL.equals(UrlBalancer.getInstance().getUrl()));
		UrlBalancer.getInstance().failUrl(failedURL);
		try {
			UrlBalancer.getInstance().getUrl();
		} catch (PEException pe) {}
		
		Thread.sleep(1500);
		
		assertTrue(failedURL.equals(UrlBalancer.getInstance().getUrl()));

		UrlBalancer.getInstance().close();
	}

	@Test (expected=PEException.class)
	public void emptyInitialListTest() throws Exception {
		UrlBalancer.getInstance().initialize(FAILED_SITE_EXPIRY, new ArrayList<String>());
		UrlBalancer.getInstance().close();
	}

	@Test (expected=PEException.class)
	public void getURLwithEmptyListTest() throws Exception {
		try {
			UrlBalancer.getInstance().initialize(FAILED_SITE_EXPIRY, new ArrayList<String>());
		} catch (PEException pe) {}
		
		UrlBalancer.getInstance().getUrl();
		UrlBalancer.getInstance().close();
	}

}
