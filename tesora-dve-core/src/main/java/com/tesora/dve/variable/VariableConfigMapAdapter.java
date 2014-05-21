// OS_STATUS: public
package com.tesora.dve.variable;

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
