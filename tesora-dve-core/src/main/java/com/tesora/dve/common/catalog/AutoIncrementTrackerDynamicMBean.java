// OS_STATUS: public
package com.tesora.dve.common.catalog;

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

public class AutoIncrementTrackerDynamicMBean implements DynamicMBean {

	@Override
	public Object getAttribute(String arg0) throws AttributeNotFoundException,
			MBeanException, ReflectionException {
		return null;
	}

	@Override
	public AttributeList getAttributes(String[] arg0) {
		return null;
	}

	@Override
	public MBeanInfo getMBeanInfo() {
		MBeanAttributeInfo[] attributes = new MBeanAttributeInfo[0];

		MBeanOperationInfo[] operations = new MBeanOperationInfo[2];
		operations[0] = new MBeanOperationInfo("dumpStats",
				"dump stats to stdout", null, Void.TYPE.getName(),
				MBeanOperationInfo.ACTION, null);

		operations[1] = new MBeanOperationInfo("clearCache",
				"clear auto-increment caches", null, Void.TYPE.getName(),
				MBeanOperationInfo.ACTION, null);

		return new MBeanInfo(this.getClass().getName(),
				"Auto-Increment Tracker MBean", attributes,
				null, // constructors
				operations, // operations
				null); // notifications
	}

	@Override
	public Object invoke(String name, Object[] args, String[] sig)
			throws MBeanException, ReflectionException {

		if ("dumpStats".equalsIgnoreCase(name)) {
			AutoIncrementTracker.dumpStats();
			return null;
		}
		if ("clearCache".equalsIgnoreCase(name)) {
			AutoIncrementTracker.clearCache();
			return null;
		}
		throw new ReflectionException(new NoSuchMethodException(name));
	}

	@Override
	public void setAttribute(Attribute arg0) throws AttributeNotFoundException,
			InvalidAttributeValueException, MBeanException, ReflectionException {
	}

	@Override
	public AttributeList setAttributes(AttributeList arg0) {
		return null;
	}

}
