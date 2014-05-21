package com.tesora.dve.externalservice;

/*
 * #%L
 * Tesora Inc.
 * Database Virtualization Engine
 * %%
 * Copyright (C) 2011 - 2014 Tesora Inc.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.log4j.Logger;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.groupmanager.GroupManager;
import com.tesora.dve.groupmanager.GroupMembershipListener.MembershipEventType;

public class ExternalServiceFactory {
	private static Logger logger = Logger.getLogger(ExternalServiceFactory.class);

	static Map<String, Entry<ExternalServicePlugin, ExternalServiceContext>> externalServiceMap = new HashMap<String, Entry<ExternalServicePlugin, ExternalServiceContext>>();

	public static ExternalServicePlugin register(String serviceName, String externalServiceClass) throws PEException {
		ExternalServicePlugin esp = null;

		GroupManager.getCoordinationServices().registerExternalService(serviceName); 

		ExternalServiceContext ctxt = new ExternalServiceContextImpl(serviceName);
		try {
			Class<?> pluginClass = Class.forName(externalServiceClass);
			Constructor<?> ctor = pluginClass.getConstructor(new Class[] {});
			esp = (ExternalServicePlugin) ctor.newInstance(new Object[] {});
			try {
				if (esp.denyServiceStart(ctxt)) {
					esp = null;
					return esp;
				}
				esp.initialize(ctxt);
				if (logger.isDebugEnabled()) {
					logger.debug("Service '" + serviceName + "' registered.");
				}
			} catch (PEException e) {
				logger.error("Unable to start external service \"" + serviceName + "\"", e);
				throw e;
			}

			externalServiceMap.put(serviceName, new AbstractMap.SimpleEntry<ExternalServicePlugin, ExternalServiceContext>(esp, ctxt));
		} catch (Exception e) {
			GroupManager.getCoordinationServices().deregisterExternalService(serviceName);
			if (logger.isDebugEnabled()) {
				logger.debug("Service '" + serviceName + "' deregistered due to failure to initialize.");
			}
			throw new PEException("Cannot instantiate ExternalService '" + externalServiceClass + "'", e);
		}
		if (ctxt.getServiceAutoStart())
			esp.start();
		return esp;
	}

	public static boolean isRegistered(String name) {
		Entry<ExternalServicePlugin, ExternalServiceContext> mapEntry = externalServiceMap.get(name);
		return ( mapEntry != null ? true : false );
	}
	
	public static ExternalServicePlugin getInstance(String name) throws PEException {
		Entry<ExternalServicePlugin, ExternalServiceContext> mapEntry = externalServiceMap.get(name);
		if ( mapEntry != null )
			return mapEntry.getKey();
		else
			throw new PEException("Service '" + name + "' isn't registered" );
	}

	public static void deregister(String name) throws PEException {
		ExternalServicePlugin esp = externalServiceMap.get(name).getKey();
		ExternalServiceContext esc = externalServiceMap.get(name).getValue();
		if ( esp == null || esc == null ) {
			throw new PEException("Could not find a registered service called '" + name + "'");
		}
		esp.close();
		esc.close();
		externalServiceMap.remove(name);
		GroupManager.getCoordinationServices().deregisterExternalService(name);
	}

	public static void closeExternalServices() throws PEException {
		Set<String> externalServiceNames = new HashSet<String>(externalServiceMap.keySet());
		for (String externalServiceName : externalServiceNames) {
			deregister(externalServiceName);
		}
		externalServiceMap.clear();
	}
	
	public static void onGroupMembershipEvent(MembershipEventType eventType, InetSocketAddress inetSocketAddress) {
		Set<String> externalServiceNames = new HashSet<String>(externalServiceMap.keySet());
		for (String externalServiceName : externalServiceNames) {
			try {
				getInstance(externalServiceName).handleGroupMembershipEvent(eventType, inetSocketAddress);
			} catch (PEException e) {
				logger.warn("Service '" + externalServiceName + "' failed to handle group membership event.", e);
			}
		}
	}
}