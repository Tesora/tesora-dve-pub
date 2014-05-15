// OS_STATUS: public
package com.tesora.dve.variable;

import java.lang.reflect.Constructor;

import javax.xml.bind.annotation.adapters.XmlAdapter;


public class VariableHandlerAdapter<VariableHandlerType> extends XmlAdapter<String, VariableHandlerType> {

	@SuppressWarnings("unchecked")
	@Override
	public VariableHandlerType unmarshal(String variableHandlerClassName) throws Exception {
		Constructor<?> ctor;
		try {
			Class<?> variableHandlerClass = Class.forName(variableHandlerClassName);
			ctor = variableHandlerClass.getConstructor(new Class[] {});
		} catch (Exception e) {
			ctor = e.getClass().getConstructor(new Class[] {String.class, Exception.class});
			Exception wrappedException = (Exception) ctor.newInstance(
					new Object[] { "Cannot instantiate variable handler " + variableHandlerClassName, e});
			throw wrappedException;
		}
		return (VariableHandlerType) ctor.newInstance(new Object[] {});
	}

	@Override
	public String marshal(VariableHandlerType v) throws Exception {
		return v.getClass().getCanonicalName();
	}

}
