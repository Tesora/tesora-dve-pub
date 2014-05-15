// OS_STATUS: public
package com.tesora.dve.tools;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Set;

import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.log4j.Logger;

import com.tesora.dve.exceptions.PEException;

public class JMXMBeanConnector {

	private static final Logger logger = Logger.getLogger(JMXMBeanConnector.class);

	JMXConnector jmxConnector;
	JMXServiceURL serviceUrl;
	MBeanServerConnection mbeanConn;

	/**
	 * Connect the client to a JMX server using the full JMX URL format. The URL
	 * should look something like:
	 * 
	 * <p>
	 * 
	 * <pre>
	 * service:jmx:rmi:///jndi/rmi://hostName:portNumber/jmxrmi
	 * </pre>
	 * 
	 * </p>
	 */
	public JMXMBeanConnector(String jmxUrl) throws PEException {
		this(jmxUrl, null, null);
	}

	/**
	 * Connect the client to a JMX server using the full JMX URL format with
	 * username/password credentials. The URL should look something like:
	 * 
	 * <p>
	 * 
	 * <pre>
	 * service:jmx:rmi:///jndi/rmi://hostName:portNumber/jmxrmi
	 * </pre>
	 * 
	 * </p>
	 */
	public JMXMBeanConnector(String jmxUrl, String userName, String password) throws PEException {
		if (jmxUrl == null) {
			throw new IllegalArgumentException("JMX URL cannot be null");
		}

		HashMap<String, Object> map = null;

		if ((userName != null) || (password != null)) {
			map = new HashMap<String, Object>();
			map.put("jmx.remote.credentials", new String[] { userName, password });
		}

		try {
			this.serviceUrl = new JMXServiceURL(jmxUrl);
		} catch (final MalformedURLException e) {
			throw new PEException("JMX Service URL was malformed: " + jmxUrl, e);
		}

		try {
			jmxConnector = JMXConnectorFactory.connect(serviceUrl, map);
			mbeanConn = jmxConnector.getMBeanServerConnection();
		} catch (final IOException e) {
			if (jmxConnector != null) {
				try {
					jmxConnector.close();
				} catch (final IOException e1) {
					logger.warn("Could not close the JMX connection", e1);
				}
				jmxConnector = null;
			}
			throw new PEException("Problems connecting to the mbean server - " + e.getMessage(), e);
		}
	}

	/**
	 * Connect the client to the local host at a certain port number.
	 */
	public JMXMBeanConnector(int localPort) throws PEException {
		this(generalJmxUrlForHostNamePort("", localPort));
	}

	/**
	 * Connect the client to a host and port combination.
	 */
	public JMXMBeanConnector(String hostName, int port) throws PEException {
		this(generalJmxUrlForHostNamePort(hostName, port));
	}

	/**
	 * Returns a JMX/RMI URL for a host-name and port.
	 */
	public static String generalJmxUrlForHostNamePort(String hostName, int port) {
		return "service:jmx:rmi:///jndi/rmi://" + hostName + ":" + port + "/jmxrmi";
	}

	/**
	 * Close the client connection to the mbean server.
	 */
	public void close() throws PEException {
		try {
			if (jmxConnector != null) {
				jmxConnector.close();
				jmxConnector = null;
			}
			mbeanConn = null;
		} catch (final IOException e) {
			throw new PEException("Could not close the JMX connector", e);
		}
	}

	/**
	 * Return an array of the bean's domain names.
	 */
	public String[] getBeanDomains() throws PEException {
		checkClientConnected();
		try {
			return mbeanConn.getDomains();
		} catch (final IOException e) {
			throw new PEException("Problems getting list of available JMX domains - " + e.getMessage(), e);
		}
	}

	/**
	 * Return a set of the various bean ObjectName objects associated with the
	 * specified domain.
	 */
	public Set<ObjectName> getBeanNames(String domain) throws PEException {
		checkClientConnected();
		try {
			return mbeanConn.queryNames(ObjectName.getInstance(domain + ":*"), null);
		} catch (final Exception e) {
			throw new PEException("Problems getting list of mbeans for domain '" + domain + "' - " + e.getMessage(), e);
		}
	}

	public MBeanInfo getBeanInfo(ObjectName name) throws PEException {
		checkClientConnected();
		try {
			return mbeanConn.getMBeanInfo(name);
		} catch (final Exception e) {
			throw new PEException("Problem getting mbean info for '" + name.toString() + "' - " + e.getMessage(), e);
		}
	}

	/**
	 * Return the value of a JMX attribute.
	 */
	public Object getAttribute(ObjectName name, String attributeName) throws PEException {
		checkClientConnected();
		try {
			return mbeanConn.getAttribute(name, attributeName);
		} catch (final Exception e) {
			throw new PEException("Problem reading attribute '" + attributeName + "' - " + e.getMessage(), e);
		}
	}

	/**
	 * Invoke a JMX method as an array of parameter strings.
	 * 
	 * @return The value returned by the method or null if none.
	 */
	public Object invokeOperation(ObjectName name, MBeanOperationInfo operation, Object[] params, String[] paramTypes) throws PEException {

		try {
			return mbeanConn.invoke(name, operation.getName(), params, paramTypes);
		} catch (final Exception e) {
			throw new PEException("Problem invoking operation '" + operation.getName() + "' - " + e.getMessage(), e);
		}
	}

	private void checkClientConnected() {
		if (mbeanConn == null) {
			throw new IllegalArgumentException("JMXMBeanConnector is not connected");
		}
	}

	public Object stringToObject(String string, String typeString) throws IllegalArgumentException {
		if (typeString.equals("boolean") || typeString.equals("java.lang.Boolean")) {
			return Boolean.parseBoolean(string);
		}

		if (typeString.equals("char") || typeString.equals("java.lang.Character")) {
			if (string.length() == 0) {
				return '\0';
			}
			return string.toCharArray()[0];
		}

		if (typeString.equals("byte") || typeString.equals("java.lang.Byte")) {
			return Byte.parseByte(string);
		}

		if (typeString.equals("short") || typeString.equals("java.lang.Short")) {
			return Short.parseShort(string);
		}

		if (typeString.equals("int") || typeString.equals("java.lang.Integer")) {
			return Integer.parseInt(string);
		}

		if (typeString.equals("long") || typeString.equals("java.lang.Long")) {
			return Long.parseLong(string);
		}

		if (typeString.equals("java.lang.String")) {
			return string;
		}

		if (typeString.equals("float") || typeString.equals("java.lang.Float")) {
			return Float.parseFloat(string);
		}

		if (typeString.equals("double") || typeString.equals("java.lang.Double")) {
			return Double.parseDouble(string);
		}

		final Constructor<?> constr = getConstructor(typeString);
		try {
			return constr.newInstance(new Object[] { string });
		} catch (final Exception e) {
			throw new IllegalArgumentException("Could not get new instance using string constructor for type "
					+ typeString);
		}
	}

	private <C> Constructor<C> getConstructor(String typeString) throws IllegalArgumentException {
		Class<Object> clazz;
		try {
			@SuppressWarnings("unchecked")
			final Class<Object> clazzCast = (Class<Object>) Class.forName(typeString);
			clazz = clazzCast;
		} catch (final ClassNotFoundException e) {
			throw new IllegalArgumentException("Unknown class for type " + typeString);
		}
		try {
			@SuppressWarnings("unchecked")
			final Constructor<C> constructor = (Constructor<C>) clazz.getConstructor(new Class[] { String.class });
			return constructor;
		} catch (final Exception e) {
			throw new IllegalArgumentException("Could not find constructor with single String argument for " + clazz);
		}
	}

	protected ObjectName makeObjectName(String name) throws PEException {
		try {
			return new ObjectName(name);
		} catch (final Exception e) {
			throw new PEException("Failed to create ObjectName - " + e.getMessage(), e);
		}
	}

	protected ObjectName makeObjectName(String domainName, String keys) throws PEException {
		return makeObjectName(domainName + ":" + keys);
	}
}
