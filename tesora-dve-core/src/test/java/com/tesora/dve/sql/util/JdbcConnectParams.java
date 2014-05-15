// OS_STATUS: public
package com.tesora.dve.sql.util;

import java.util.Properties;

import com.tesora.dve.common.PEUrl;
import com.tesora.dve.exceptions.PEException;

public class JdbcConnectParams {
	private PEUrl peurl;
	private String userid;
	private String password;
	
	JdbcConnectParams(String url, String userid, String password) throws PEException {
		this.peurl = PEUrl.fromUrlString(url);
		this.userid = userid;
		this.password = password;
	}

	public JdbcConnectParams(JdbcConnectParams other) {
		this.peurl = other.peurl;
		this.userid = other.userid;
		this.password = other.password;
	}
	
	public String getUrl() throws PEException {
		return peurl.getURL();
	}
	
	public PEUrl getPEUrl() {
		return peurl;
	}
	
	public void setUrl(String url) throws PEException {
		this.peurl = PEUrl.fromUrlString(url);
	}
	
	public String getUserid() {
		return userid;
	}
	
	public void setUserid(String userid) {
		this.userid = userid;
	}
	
	public String getPassword() {
		return password;
	}
	
	public void setPassword(String password) {
		this.password = password;
	}
	
	public void setURLOptions(Properties urlOptions) throws Throwable {
		peurl.setQueryOptions(urlOptions);
	}
}