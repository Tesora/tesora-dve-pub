// OS_STATUS: public
package com.tesora.dve.variable;

import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.annotation.adapters.XmlAdapter;

public class VariableConfigMapAdapter<VariableHandlerType extends VariableHandler>
			extends XmlAdapter<VariableList<VariableHandlerType>, Map<String, VariableInfo<VariableHandlerType>>> {

	@Override
	public Map<String, VariableInfo<VariableHandlerType>> unmarshal(VariableList<VariableHandlerType> v) throws Exception {
		Map<String, VariableInfo<VariableHandlerType>> vMap = new HashMap<String, VariableInfo<VariableHandlerType>>();
		for (VariableInfo<VariableHandlerType> vInfo : v.variableList)
			vMap.put(VariableConfig.canonicalise(vInfo.name), vInfo);
		return vMap;
	}

	@Override
	public VariableList<VariableHandlerType> marshal(Map<String, VariableInfo<VariableHandlerType>> v) throws Exception {
		return new VariableList<VariableHandlerType>(v.values());
	}

}
