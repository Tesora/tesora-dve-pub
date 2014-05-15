// OS_STATUS: public
package com.tesora.dve.variable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.xml.bind.annotation.XmlElement;

public class VariableList<VariableHandlerType extends VariableHandler> {


	public VariableList(Collection<VariableInfo<VariableHandlerType>> values) {
		variableList = new ArrayList<VariableInfo<VariableHandlerType>>(values);
	}

	public VariableList() {
	}

	@XmlElement(name="Variable")
	List<VariableInfo<VariableHandlerType>> variableList;
}
