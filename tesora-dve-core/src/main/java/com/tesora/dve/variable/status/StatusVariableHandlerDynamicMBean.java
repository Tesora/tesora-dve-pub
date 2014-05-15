// OS_STATUS: public
package com.tesora.dve.variable.status;

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
import javax.management.MBeanOperationInfo;
import javax.management.ReflectionException;

import com.tesora.dve.common.catalog.CatalogDAO;
import com.tesora.dve.common.catalog.CatalogDAO.CatalogDAOFactory;

public class StatusVariableHandlerDynamicMBean implements DynamicMBean {

	private Map<String, StatusVariableHandler> variables = new HashMap<String, StatusVariableHandler>();

	public void add(String name, StatusVariableHandler variable) {
		variables.put(name, variable);
	}

	@Override
	public String getAttribute(String name) throws AttributeNotFoundException {

		StatusVariableHandler variable = variables.get(name);

		if (variable == null)
			throw new AttributeNotFoundException("No such variable: " + name);

		CatalogDAO c = CatalogDAOFactory.newInstance();
		try {
			return variable.getValue(c, variable.variableName);
		} catch (Exception e) {
			throw new AttributeNotFoundException("Failed to get value on variable: " + name);
		} finally {
			c.close();
		}
	}

	@Override
	public void setAttribute(Attribute attribute) throws InvalidAttributeValueException, MBeanException,
			AttributeNotFoundException {

		throw new AttributeNotFoundException("Set attribute not allowed on this variable");
	}

	@Override
	public AttributeList getAttributes(String[] names) {
		CatalogDAO c = CatalogDAOFactory.newInstance();
		try {
			AttributeList list = new AttributeList();
			for (Entry<String, StatusVariableHandler> entry : variables.entrySet()) {
				StatusVariableHandler handler = entry.getValue();
				list.add(new Attribute(entry.getKey(), handler.getValue(c, handler.variableName)));
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
		return new AttributeList();
	}

	@Override
	public Object invoke(String name, Object[] args, String[] sig) throws MBeanException, ReflectionException {
		if ("reset".equalsIgnoreCase(name)) {
			reset();
			return null;
		}
		throw new ReflectionException(new NoSuchMethodException(name));
	}

	public void reset() {
		CatalogDAO c = CatalogDAOFactory.newInstance();
		try {
			for (StatusVariableHandler handler : variables.values()) {
				try {
					handler.reset(c, handler.variableName);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} finally {
			c.close();
		}
	}

	@Override
	public MBeanInfo getMBeanInfo() {

		MBeanAttributeInfo[] attributes = new MBeanAttributeInfo[variables.size()];
		int i = 0;
		for (Entry<String, StatusVariableHandler> entry : variables.entrySet()) {
			attributes[i++] = new MBeanAttributeInfo(entry.getKey(), "java.lang.String", "variable " + entry.getKey(),
					true, // isReadable
					false, // isWritable
					false); // isIs

		}

		MBeanOperationInfo[] operations = new MBeanOperationInfo[1];
		operations[0] = new MBeanOperationInfo("reset", "reset status variables to zero", null, Void.TYPE.getName(),
				MBeanOperationInfo.ACTION, null);

		return new MBeanInfo(this.getClass().getName(), "Global Status Variable MBean", attributes, null, // constructors
				operations, // operations
				null); // notifications
	}
}