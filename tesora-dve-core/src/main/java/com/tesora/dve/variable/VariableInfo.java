// OS_STATUS: public
package com.tesora.dve.variable;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PENotFoundException;


@XmlRootElement(name="Variable")
@XmlAccessorType(XmlAccessType.NONE)
public class VariableInfo<VariableHandlerType extends VariableHandler> {
	
	@XmlAttribute
	String name;
	
	@XmlAttribute
	String defaultValue;
	
	@XmlAttribute
	@XmlJavaTypeAdapter(VariableHandlerAdapter.class)
	VariableHandlerType handler;

	public VariableInfo() {
		super();
	}

	public VariableInfo(String name, String defaultValue,
			Class<VariableHandlerType> handler) throws PEException {
		super();
		this.name = name;
		this.defaultValue = defaultValue;
		try {
			this.handler = handler.getConstructor(new Class[] {}).newInstance(new Object[] {});
		} catch (NoSuchMethodException e) {
			throw new PENotFoundException("Handler " + handler.getSimpleName() + " does not have a no-args constructor", e);
		} catch (Exception e) {
			throw new PEException("Unable to invoke constructor on " + handler.getCanonicalName(), e);
		}
	}

	public String getName() {
		return name;
	}

	public String getDefaultValue() {
		return defaultValue;
	}

	public VariableHandlerType getHandler() {
		return handler;
	}

}
