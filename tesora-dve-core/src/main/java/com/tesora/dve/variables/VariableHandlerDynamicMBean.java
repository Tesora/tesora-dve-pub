package com.tesora.dve.variables;

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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.ReflectionException;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;
import com.tesora.dve.server.connectionmanager.SSConnectionProxy;
import com.tesora.dve.server.global.HostService;
import com.tesora.dve.singleton.Singletons;
import com.tesora.dve.sql.schema.VariableScopeKind;

public class VariableHandlerDynamicMBean implements DynamicMBean {

	private Map<String, VariableHandler> variables = 
			new HashMap<String, VariableHandler>();

	public void add(String name, VariableHandler<?> variable) {
		variables.put(name, variable);
	}

	@Override
	public String getAttribute(String name) throws AttributeNotFoundException {

		VariableHandler variable = variables.get(name);

		if (variable == null)
			throw new AttributeNotFoundException("No such variable: " + name);

		return variable.toExternal(variable.getValue(null,VariableScopeKind.GLOBAL));
	}

	@Override
	public void setAttribute(Attribute attribute) throws InvalidAttributeValueException, MBeanException,
			AttributeNotFoundException {
		
		VariableHandler<?> variable = variables.get(attribute.getName());

		if (variable == null)
			throw new AttributeNotFoundException("No such variable: " + attribute.getName());

		Object value = attribute.getValue();
		if (!(value instanceof String)) {
			throw new InvalidAttributeValueException("Attribute value not a string: " + value);
		}

		SSConnectionProxy prox = null;
		
		try { 
			prox = Singletons.require(HostService.class).getRootProxy();
			variable.setGlobalValue(prox.getConnection(),(String)value);
		} catch (Throwable t) {
			throw new InvalidAttributeValueException("Failed to set value on variable: " + attribute.getName() + " (" + t.getMessage() + ")");
		} finally {
			if (prox != null) try {
				prox.close();
			} catch (Throwable t) {
				// did our best
			}
		}
	}

	@Override
	public AttributeList getAttributes(String[] names) {
		CatalogDAO c = CatalogDAOFactory.newInstance();
		try {
			AttributeList list = new AttributeList();
			for (Entry<String, VariableHandler> entry : variables.entrySet()) {
				list.add(new Attribute(entry.getKey(),
						entry.getValue().toExternal(entry.getValue().getGlobalValue(null))));
			}
			return list;
		} catch (Exception e) {
			throw new RuntimeException("Failed to get attributes  - " + e.getMessage());
		} finally {
			c.close();
		}
	}

	@Override
	public AttributeList setAttributes(AttributeList list) {
		Attribute[] attrs = (Attribute[]) list.toArray(new Attribute[0]);

		AttributeList retlist = new AttributeList();

		SSConnectionProxy prox = null;

		
		try {
			prox = Singletons.require(HostService.class).getRootProxy();
			for (Attribute attr : attrs) {
				String name = attr.getName();
				Object value = attr.getValue();

				if (variables.get(name) != null && value instanceof String) {
					VariableHandler<?> vh = variables.get(name);
					vh.setGlobalValue(prox.getConnection(),(String)value);
					retlist.add(new Attribute(name, value));
				}
			}
			return retlist;
		} catch (Exception e) {
			throw new RuntimeException("Failed to set attributes  - " + e.getMessage());
		} finally {
			if (prox != null) try {
				prox.close();
			} catch (Throwable t) {
				// did our best
			}
		}
	}

	@Override
	public Object invoke(String name, Object[] args, String[] sig) throws MBeanException, ReflectionException {
		throw new ReflectionException(new NoSuchMethodException(name));
	}

	@Override
	public MBeanInfo getMBeanInfo() {

		MBeanAttributeInfo[] attributes = new MBeanAttributeInfo[variables.size()];
		int i = 0;
		for (Entry<String, VariableHandler> entry : variables.entrySet()) {
			attributes[i++] = new MBeanAttributeInfo(
					entry.getKey(), 
					"java.lang.String", 
					"variable " + entry.getKey(),
					true, // isReadable
					true, // isWritable
					false); // isIs

		}

		return new MBeanInfo(this.getClass().getName(), 
				"Global Config Variable MBean", 
				attributes, 
				null,  // constructors
				null,  // operations
				null); // notifications
	}
}