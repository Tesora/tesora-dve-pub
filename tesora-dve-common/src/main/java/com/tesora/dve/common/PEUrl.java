// OS_STATUS: public
package com.tesora.dve.common;

import java.util.Properties;

import org.apache.commons.lang.StringUtils;

import com.tesora.dve.exceptions.PEException;

public class PEUrl {

	private String protocol;
	private String subProtocol;
	private String host;
	private int port;
	private String path;
	private String query;
	private Properties queryOptions = new Properties();
	private boolean hasPort = false;

	/**
	 * Create an uninitialized instance of PEUrl.
	 */
	public PEUrl() {
	}

	/**
	 * Create an instance of PEUrl and initialize with values from Properties
	 * object. If the Properties contains a prefix."url" key, PEUrl will be
	 * initialized with that; otherwise it will look for prefix."host",
	 * prefix."port", prefix."type" and prefix."dbname"
	 * 
	 * This doesn't currently handle passing query options via a properties file
	 * 
	 * @param Properties
	 *            props
	 * @param String
	 *            prefix
	 * @throws PEException
	 *             if the ".url" is malformed or the props doesn't contain
	 *             enough info to make a valid URL.
	 */
	public PEUrl(Properties props, String prefix) throws PEException {
		this(props, prefix, false);
	}

	/**
	 * Create an instance of PEUrl and initialize with values from Properties
	 * object. If the Properties contains a prefix."url" key, PEUrl will be
	 * initialized with that; otherwise it will look for prefix."host",
	 * prefix."port", prefix."type" and prefix."dbname". If setDefaults is true,
	 * then defaults will be filled in for host (localhost), port (3306) and
	 * type (mysql).
	 * 
	 * This doesn't currently handle passing query options via a properties file
	 * 
	 * @param Properties
	 *            props
	 * @param String
	 *            prefix
	 * @param boolean useDefaults
	 * @throws PEException
	 *             if the ".url" is malformed.
	 */

	public PEUrl(Properties props, String prefix, boolean useDefaults) throws PEException {
		initializeFromProps(props, prefix, useDefaults);

		if (!isInitialized()) {
			throw new PEException("Error creating URL object from Properties: " + props.toString());
		}

	}

	/**
	 * Initialize with the information contained in <code>urlString</code>
	 * 
	 * @param urlString
	 *            - a String representing a valid URL
	 * @throws PEException
	 *             - if <code>urlString</code> is not a properly formed URL
	 */
	public static PEUrl fromUrlString(String urlString) throws PEException {
		PEUrl url = new PEUrl();

		url.parseURL(urlString);

		if (!url.isInitialized())
			throw new PEException("Error creating URL object from string: " + urlString);

		return url;
	}

	public static PEUrl isValidUrlWithPort(String urlString) throws PEException {

		// This is a test to ensure the url is well formed
		PEUrl pUrl = fromUrlString(urlString);

		// PEUrl will have done some checking - but we want to ensure we
		// have a valid port
		if (!pUrl.hasPort())
			throw new PEException("URL '" + urlString + "' does not contain a valid port");
		
		return pUrl;
	}

	/**
	 * Initialize with the information contained in <code>connectString</code>
	 * 
	 * @param connectString
	 *            - a String representing parameters for a URL in
	 *            "connect string" format. e.g.
	 *            "host=localhost;port=3306;dbname=dve_catalog"
	 * @throws PEException
	 *             - if <code>connectString</code> is malformed - connectString
	 *             must contain at least a "host" token
	 * 
	 */
	public static PEUrl fromConnectString(String connectString) throws PEException {
		PEUrl url = new PEUrl();

		// TODO this "parser" will not handle embedded delimeters
		String delims = "[=;]";
		String[] tokens = connectString.split(delims);

		if (tokens.length % 2 != 0)
			throw new PEException("Connect String does not contain the correct number of elements");

		Properties props = new Properties();
		for (int i = 0; i < tokens.length; i = i + 2) {
			props.setProperty(tokens[i], tokens[i + 1]);
		}

		url.initializeFromProps(props, null /* prefix */, false /* useDefaults */);

		if (!url.isInitialized())
			throw new PEException("Error creating URL object from connect string: " + connectString);

		return url;
	}

	/**
	 * Is this PEUrl instance properly initialized. Minimally, this means that
	 * the protocol, subProtocol and host are set. (set via PEUrl(urlString)
	 * constructor or calling setXXX methods
	 * 
	 * @return boolean indicating if the instance is properly initialized
	 */
	boolean isInitialized() {
		return (protocol != null && subProtocol != null && host != null && !host.isEmpty());
	}

	/**
	 * Will populate an empty PEUrl instance with the default URL parameters for
	 * a MySQL JDBC connection. Calling example:
	 * <code>PEUrl url = new PEUrl().createMysqlDefaultURL()</code>
	 * 
	 * @return PEUrl instance representing "jdbc:mysql://localhost:3306"
	 */
	public PEUrl createMysqlDefaultURL() {
		setProtocol(PEConstants.MYSQL_PROTOCOL);
		setSubProtocol(PEConstants.MYSQL_SUBPROTOCOL);
		setHost(PEConstants.MYSQL_HOST);
		setPort(PEConstants.MYSQL_PORT);
		return this;
	}

	/**
	 * Gets a queryOption from the option properties based on a key value.
	 * 
	 * @param optionKey
	 *            - the key for the option to return
	 * @return String - value of option for key <code>optionKey</code>
	 */
	public String getOption(String optionKey) {
		return queryOptions.getProperty(optionKey);
	}

	/**
	 * Returns a string representation of the (i.e. a URL) of the current state
	 * of this PEUrl instance. Instance must be properly initialized - meaning
	 * that at least the protocol, subProtocol and host attributes must be set.
	 * 
	 * @return String representation of URL
	 * @throws PEException
	 *             - if instance isn't properly initialized.
	 */
	public String getURL() throws PEException {
		if (!isInitialized())
			throw new PEException("Cannot call getURL method when PEUrl object is not initialized");

		StringBuffer sb = new StringBuffer();

		sb.append(protocol).append(':').append(subProtocol).append("://").append(getAuthority());

		if (path != null)
			sb.append('/').append(path);

		if (query != null)
			sb.append('?').append(query);

		return sb.toString();
	}

	public String getProtocol() {
		return protocol;
	}

	/**
	 * Set the protocol to <code>protocol</code>
	 * 
	 * @param protocol
	 * @return PEUrl this
	 */
	public PEUrl setProtocol(String protocol) {
		this.protocol = protocol;
		return this;
	}

	public String getSubProtocol() {
		return subProtocol;
	}

	/**
	 * Set the sub protocol to <code>subProtocol</code>
	 * 
	 * @param subProtocol
	 * @return PEUrl this
	 */
	public PEUrl setSubProtocol(String subProtocol) {
		this.subProtocol = subProtocol;
		return this;
	}

	public String getAuthority() {
		return host + (hasPort ? ":" + port : "");
	}

	/**
	 * Set the authority to <code>authority</code> This set the host and port as
	 * appropriate. e.g. calling <code>setAuthority("localhost:6800")</code>
	 * will set host to "localhost" and port to 6800
	 * 
	 * @param authority
	 * @return PEUrl this
	 */
	public void setAuthority(String authority) {
		String[] authorityA = authority.split(":");
		if (authorityA.length == 2) {
			hasPort = true;
			port = Integer.parseInt(authorityA[1]);
		}

		host = authorityA[0];
	}

	public String getHost() {
		return host;
	}

	/**
	 * Set the host to <code>host</code>.
	 * 
	 * @param host
	 * @return PEUrl this
	 */
	public PEUrl setHost(String host) {
		this.host = host;
		return this;
	}

	public int getPort() {
		return port;
	}

	/**
	 * Set the port to <code>port</code>.
	 * 
	 * @param int port
	 * @return PEUrl this
	 */
	public PEUrl setPort(int port) {
		hasPort = true;
		this.port = port;
		return this;
	}

	public boolean hasPort() {
		return hasPort;
	}

	/**
	 * Set the port to <code>port</code>.
	 * 
	 * @param String
	 *            port
	 * @return PEUrl this
	 */
	public PEUrl setPort(String port) {
		setPort(Integer.parseInt(port));
		return this;
	}

	public String getPath() {
		return path;
	}

	/**
	 * Set the path to <code>path</code>.
	 * 
	 * @param String
	 *            path
	 * @return PEUrl this
	 */
	public PEUrl setPath(String path) {
		this.path = path;
		return this;
	}

	public String getQuery() {
		return query;
	}

	/**
	 * Set the query options to <code>query</code>. Query options are of the
	 * form "opt1Key=optValue1&opt2Key=optValue2"
	 * 
	 * @param String
	 *            query
	 * @return PEUrl this
	 */
	public PEUrl setQuery(String query) throws PEException {
		this.query = query;
		queryOptions = parseURLQuery(query);
		return this;
	}

	/**
	 * Return all the query options as <code>Properties</code>
	 * 
	 * @return Properties - the query options
	 */
	public Properties getQueryOptions() {
		return queryOptions;
	}

	/**
	 * Set one query option for the URL.
	 * 
	 * @param key
	 *            for Option
	 * @param value
	 *            for Option
	 * @return PEUrl this
	 */
	public PEUrl setQueryOption(String key, String value) {
		queryOptions.setProperty(key, value);
		calcQueryString();
		return this;
	}

	public PEUrl setQueryOptions(Properties props) {
		for (String key : props.stringPropertyNames()) {
			setQueryOption(key, props.getProperty(key));
		}

		return this;
	}

	private void initializeFromProps(Properties props, String prefix, boolean useDefaults) throws PEException {
		// TODO handle query options

		String normPrefix = PEStringUtils.normalizePrefix(prefix);

		if (props.getProperty(normPrefix + "url") != null) {
			parseURL(props.getProperty(normPrefix + "url"));
		} else {
			if (useDefaults)
				createMysqlDefaultURL();
			else {
				setProtocol(PEConstants.MYSQL_PROTOCOL);
				setSubProtocol(PEConstants.MYSQL_SUBPROTOCOL);
			}
			if (props.getProperty(normPrefix + "host") != null)
				setHost(props.getProperty(normPrefix + "host"));
			if (props.getProperty(normPrefix + "port") != null)
				setPort(props.getProperty(normPrefix + "port"));
			if (props.getProperty(normPrefix + "dbname") != null)
				setPath(props.getProperty(normPrefix + "dbname"));
		}
	}

	private PEUrl parseURL(String urlString) throws PEException {

		if (StringUtils.isEmpty(urlString))
			throw new PEException("Can't parse an empty URL");

		hasPort = false;
		// parse protocol from beginning (e.g. jdbc:mysql
		String[] protocolA = urlString.split(":", 3);
		if (protocolA.length != 3)
			throw new PEException("Malformed URL '" + urlString + "' - incomplete protocol");

		setProtocol(protocolA[0]);
		setSubProtocol(protocolA[1]);

		// next, parse authority (i.e. host:port - localhost:6800)
		String[] authorityA = protocolA[2].split("//", 2);
		if (authorityA.length != 2 || !authorityA[0].isEmpty())
			throw new PEException("Malformed URL '" + urlString + "' - invalid authority");

		// next, determine is query exists ("?")
		String[] queryA = authorityA[1].split("\\?", 2);
		if (queryA.length == 2) {
			setQuery(queryA[1]);
		}

		// next, parse path, if it exists (i.e. dbname)
		String[] pathA = queryA[0].split("/", 2);

		String[] hostA = pathA[0].split(":");
		// split authority into host and port
		if (hostA.length == 2) {
			setPort(hostA[1]);
		}

		setHost(hostA[0]);

		// if a path exists, store it
		if (pathA.length == 2)
			setPath(pathA[1]);

		return this;
	}

	private Properties parseURLQuery(String urlQuery) throws PEException {
		Properties urlQProps = new Properties();

		String[] params = urlQuery.split("&");
		for (String param : params) {
			String[] queryElems = param.split("=");
			if (queryElems.length != 2)
				throw new PEException("Invalid options specified on URL");
			urlQProps.setProperty(queryElems[0], queryElems[1]);
		}

		return urlQProps;
	}

	private void calcQueryString() {
		query = "";
		for (String key : queryOptions.stringPropertyNames()) {
			if (!query.isEmpty())
				query += "&";

			query += key + "=" + queryOptions.getProperty(key);
		}
	}

	@Override
	public String toString() {
		try {
			return getURL();
		} catch (Exception e) {
			// ignore
		}
		return super.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((host == null) ? 0 : host.hashCode());
		result = prime * result + ((path == null) ? 0 : path.hashCode());
		result = prime * result + port;
		result = prime * result + ((protocol == null) ? 0 : protocol.hashCode());
		result = prime * result + ((query == null) ? 0 : query.hashCode());
		result = prime * result + ((queryOptions == null) ? 0 : queryOptions.hashCode());
		result = prime * result + ((subProtocol == null) ? 0 : subProtocol.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PEUrl other = (PEUrl) obj;
		if (host == null) {
			if (other.host != null)
				return false;
		} else if (!host.equals(other.host))
			return false;
		if (path == null) {
			if (other.path != null)
				return false;
		} else if (!path.equals(other.path))
			return false;
		if (port != other.port)
			return false;
		if (protocol == null) {
			if (other.protocol != null)
				return false;
		} else if (!protocol.equals(other.protocol))
			return false;
		if (query == null) {
			if (other.query != null)
				return false;
		} else if (!query.equals(other.query))
			return false;
		if (queryOptions == null) {
			if (other.queryOptions != null)
				return false;
		} else if (!queryOptions.equals(other.queryOptions))
			return false;
		if (subProtocol == null) {
			if (other.subProtocol != null)
				return false;
		} else if (!subProtocol.equals(other.subProtocol))
			return false;
		return true;
	}
}
