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

import java.io.File;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import org.apache.log4j.Logger;

import com.tesora.dve.exceptions.PEException;
import com.tesora.dve.exceptions.PENotFoundException;

@XmlRootElement(name="VariableConfig")
@XmlAccessorType(XmlAccessType.NONE)
public class VariableConfig<VariableHandlerType extends VariableHandler> {
	
	static Logger logger = Logger.getLogger("com.tesora.dve.variable.VariableConfig");

	@XmlElement(name="VariableList")
	@XmlJavaTypeAdapter(VariableConfigMapAdapter.class)
	Map<String /* var name */, VariableInfo<VariableHandlerType>> variableMap 
		= new HashMap<String, VariableInfo<VariableHandlerType>>();

	public Set<Entry<String, VariableInfo<VariableHandlerType>>> getInfoEntrySet() {
		return variableMap.entrySet();
	}	
	
	public Collection<VariableInfo<VariableHandlerType>> getInfoValues() {
		return variableMap.values();
	}	
	
	@SuppressWarnings("unchecked")
	public <HandlerType extends VariableHandlerType> void add(VariableInfo<HandlerType> vInfo) {
		variableMap.put(canonicalise(vInfo.getName()), (VariableInfo<VariableHandlerType>) vInfo);
	}
	
	public VariableInfo<VariableHandlerType> getVariableInfo(String variableName) throws PENotFoundException {
		return getVariableInfo(variableName, true);
	}
	
	public VariableInfo<VariableHandlerType> getVariableInfo(String variableName, boolean except) throws PENotFoundException {
		String key = canonicalise(variableName);
		if (!variableMap.containsKey(key)) {
			String notFoundMessage = "Variable \"" + key + "\" not found";
			if (logger.isDebugEnabled())
				logger.warn(notFoundMessage);
			if (except)
				throw new PENotFoundException(notFoundMessage);
		}
		return variableMap.get(key);
	}

	public static String canonicalise(String name) {
		return name.toLowerCase(Locale.ENGLISH);
	}

	@SuppressWarnings("rawtypes")
	public static VariableConfig parseXML(String filePath) throws PEException {
		VariableConfig variableConfig;
		try {
			InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(filePath);
			if (is == null)
				throw new PEException("Cannot load " + filePath + " from classpath");
			JAXBContext jc = JAXBContext.newInstance(VariableConfig.class);
			Unmarshaller u = jc.createUnmarshaller();
			variableConfig =  (VariableConfig) u.unmarshal(is);
			if (variableConfig.variableMap.isEmpty())
				throw new PEException(filePath+ " contained no variable configurations");
		} catch (JAXBException e) {
			throw new PEException("JAXB exception reading " + filePath, e);
		}
		return variableConfig;
	}

	// To generate an example StaticSiteManager configuration file
	public static void main(String[] args) {
		VariableConfig<GlobalVariableHandler> smc = new VariableConfig<GlobalVariableHandler>();
		try {
			smc.add(new VariableInfo<ConfigVariableHandler>("tvar", "defvalue", ConfigVariableHandler.class));
			smc.add(new VariableInfo<ConfigVariableHandler>("tvar2", "defvalue", ConfigVariableHandler.class));

			try {
				JAXBContext jc = JAXBContext.newInstance(VariableConfig.class);
				Marshaller m = jc.createMarshaller();
				m.setProperty( Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE );
				m.marshal(smc, new File("configSample.xml"));
			} catch (Exception e) {
				e.printStackTrace();
			}
		} catch (PEException e1) {
			e1.printStackTrace();
		}
	}
}
